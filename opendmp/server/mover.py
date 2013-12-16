'''This module create a socket for Mover operations
and process the data stream'''

import socket, threading, sys, traceback, errno
from tools import ipaddress as ip
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt

class Mover(threading.Thread):
    def __init__(self, record):
        threading.Thread.__init__(self, name='Mover-' + repr(threading.activeCount()))
        self.record = record
    
    def run(self):
        stdlog.info('starting mover operation')
        try:
            if(self.record.mover['addr_type'] == const.NDMP_ADDR_LOCAL):
                self.fd = ip.get_next_data_conn(loopback=True)
            else:
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
        except OSError as e:
            stdlog.error(e)
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
                self.record.device.data = self.sock.recv(int(cfg['BUFSIZE']))
                self.record.device.write(self.record)
                #with self.record.mover['lock']:
                self.record.mover['bytes_moved'][0] += len(self.record.device.data)
                if not self.record.device.data:
                    self.terminate()
            except (OSError, IOError) as e:
                stdlog.error(self.record.device.path + ': ' + e.strerror)
                if(e.errno == errno.EACCES):
                    self.record.error = const.NDMP_WRITE_PROTECT_ERR
                    self.terminate()
                elif(e.errno == errno.ENOENT):
                    self.record.error = const.NDMP_NO_DEVICE_ERR
                    self.terminate()
                elif(e.errno == errno.EBUSY):
                    self.record.error = const.NDMP_DEVICE_BUSY_ERR
                    self.terminate()
                elif(e.errno == errno.ENODEV):
                    self.record.error = const.NDMP_NO_DEVICE_ERR
                    self.terminate()
                elif(e.errno == errno.ENOSPC):
                    self.record.error = const.NDMP_EOM_ERR
                    with self.record.mover['lock']:
                        self.record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_EOM
                    self.pause()
                else:
                    self.record.error = const.NDMP_IO_ERR
                    self.terminate()
    
    def recover(self):
        self.record.device.count = self.record.mover['record_size']
        while not self.record.mover['equit'].is_set():
            try:
                self.record.device.read(self.record)
                #with self.record.mover['lock']:
                self.record.mover['bytes_moved'][0] += self.sock.send(self.record.device.data)
                if not self.record.device.data:
                    self.terminate()
            except (OSError, IOError) as e:
                stdlog.error(self.record.device.path + ': ' + e.strerror)
                if(e.errno == errno.EACCES):
                    self.record.error = const.NDMP_WRITE_PROTECT_ERR
                    self.terminate()
                elif(e.errno == errno.ENOENT):
                    self.record.error = const.NDMP_NO_DEVICE_ERR
                    self.terminate()
                elif(e.errno == errno.EBUSY):
                    self.record.error = const.NDMP_DEVICE_BUSY_ERR
                    self.terminate()
                elif(e.errno == errno.ENODEV):
                    self.record.error = const.NDMP_NO_DEVICE_ERR
                    self.terminate()
                elif(e.errno == errno.ENOSPC):
                    self.record.error = const.NDMP_EOM_ERR
                    with self.record.mover['lock']:
                        self.record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_EOM
                    self.pause()
                else:
                    self.record.error = const.NDMP_IO_ERR
                    self.terminate()
                
    def pause(self):
        with self.record.mover['lock']:
            stdlog.info('MOVER> pausing status ' + repr(const.ndmp_error[self.record.error]))
            self.record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
            self.record.queue.put(nt.mover_paused().post(self.record))
        self.record.mover['econt'].wait() # Wait for the DMA order to continue the operation
        self.backup()
            
    def terminate(self):
        self.record.mover['equit'].clear()
        try:
            self.fd.close()
        except socket.error as e:
            stdlog.error(repr(e))
        with self.record.mover['lock']:
            stdlog.info('MOVER> closing status ' + repr(const.ndmp_error[self.record.error]))
            self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        self.record.queue.put(nt.mover_halted().post(self.record))
        sys.exit()