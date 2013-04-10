'''This module create a socket for Mover operations
and process the data stream'''

import socket, threading, sys, traceback, time
from tools import ipaddress as ip
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt

class Mover(threading.Thread):
    def __init__(self, record):
        threading.Thread.__init__(self, name='Mover-' + repr(threading.activeCount()))
        self.record = record
    
    def run(self):
        stdlog.info('starting mover operation')
        try:
            self.fd = ip.get_next_data_conn()
        except:
            self.record.error = const.NDMP_CONNECT_ERR
            stdlog.info('Cannot get IP')
            stdlog.debug(traceback.print_exc())
        (host, port) = self.fd.getsockname()
        self.record.mover['qinfos'].put(ip.ip_address(host)._ip_int_from_string(host))
        self.record.mover['qinfos'].put(port)
        self.record.mover['qinfos']
        with self.record.mover['lock']:
            self.record.mover['state'] = const.NDMP_MOVER_STATE_LISTEN
            
        try:
            self.sock, self.addr = self.fd.accept()
        except socket.error as e:
            stdlog.error(repr(e))
            self.terminate()
            
        with self.record.mover['lock']:
            self.record.mover['state'] = const.NDMP_MOVER_STATE_ACTIVE
        
        if self.record.mover['mode'] == const.NDMP_MOVER_MODE_READ:
            self.backup()
        elif self.record.mover['mode'] == const.NDMP_MOVER_MODE_WRITE:
            self.recover()
                
    def backup(self):
        while not self.record.mover['equit'].is_set():
            try:
                self.record.device.data = self.sock.recv(self.record.mover['record_size'])
                self.record.device.write(self.record)
                with self.record.mover['lock']:
                    self.record.mover['bytes_moved'] += len(self.record.device.data)
                if not self.record.device.data:
                    self.terminate()
            except socket.error as e:
                stdlog.error(repr(e))
                self.terminate()
            except (OSError, IOError) as e:
                stdlog.error('Mover write failed: ' + e.strerror)
                self.terminate()
    
    def recover(self):
        self.record.device.count = self.record.mover['record_size']
        while not self.record.mover['equit'].is_set():
            try:
                self.record.device.read(self.record)
                with self.record.mover['lock']:
                    self.record.mover['bytes_moved'] += self.sock.send(self.record.device.data)
                if not self.record.device.data:
                    self.terminate()
            except socket.error as e:
                stdlog.error(repr(e))
                self.terminate()
            except (OSError, IOError) as e:
                stdlog.error('Mover read failed: ' + e.strerror)
                self.terminate()
            
    def terminate(self):
        try:
            self.fd.close()
        except socket.error as e:
            stdlog.error(repr(e))
        with self.record.mover['lock']:
            stdlog.info('closing status ' + repr(const.ndmp_error[self.record.error]))
            self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        self.record.queue.put(nt.mover_halted().post(self.record))
        sys.exit()