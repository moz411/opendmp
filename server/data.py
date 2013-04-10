'''This module create a socket for Data operations
and process the data stream'''

import socket, threading, sys, os, time
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt, fh


class Data(threading.Thread):
    def __init__(self, record):
        threading.Thread.__init__(self, name='Data-' + repr(threading.activeCount()))
        self.record = record
        
    def run(self):
        stdlog.info('starting data operation')
            
        if self.record.data['operation'] == const.NDMP_DATA_OP_BACKUP:
            self.backup()
        elif self.record.data['operation'] == const.NDMP_DATA_OP_RECOVER:
            self.recover()
        
    def backup(self):
        while not self.record.data['equit'].is_set():
            try:
                data = self.record.data['process'].stdout.read(10240)
                with self.record.data['lock']:
                    self.record.data['stats']['current'] += self.record.data['fd'].send(data)
                if not data:
                    self.record.data['fd'].send(data)
                    self.terminate()
            except socket.error as e:
                stdlog.error(repr(e))
                self.terminate()
            except (OSError, IOError) as e:
                stdlog.error('Data read failed: ' + e.strerror)
                stdlog.error(bytes(self.record.data['process'].stderr.read()).decode())
                self.terminate()
                
    def recover(self):
        while not self.record.data['equit'].is_set():
            try:
                data = self.record.data['fd'].recv(10240)
                self.record.data['process'].stdin.write(data)
                with self.record.data['lock']:
                    self.record.data['stats']['current'] += len(data)
                if not data:
                    self.terminate()
            except socket.error as e:
                stdlog.error(repr(e))
                self.terminate()
            except (OSError, IOError) as e:
                stdlog.error('Data write failed: ' + e.strerror)
                stdlog.error(bytes(self.record.data['process'].stderr.read()).decode())
                self.terminate()
    
    def terminate(self):
        self.record.data['process'].communicate()
        try:
            self.record.data['fd'].close()
        except socket.error as e:
            stdlog.error(repr(e))
        
        with self.record.data['lock']:
            stdlog.info('closing status ' + repr(self.record.data['process'].returncode))
            self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
        self.record.data['process'].wait()
        self.record.data['process'].poll()
        with self.record.data['lock']:
            if (self.record.data['process'].returncode == 0):
                    self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
                    self.record.queue.put(fh.add_file().post(self.record))
            else:
                    self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
        self.record.queue.put(nt.data_halted().post(self.record))
        sys.exit()

        
        