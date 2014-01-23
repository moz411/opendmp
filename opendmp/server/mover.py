'''This module create a asyncore Consumer for Mover operations
and process the data stream'''

import asyncore, errno
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt

class Mover(asyncore.dispatcher):
    
    def __init__(self, connection, record):
        asyncore.dispatcher.__init__(self, connection)
        self.connection = connection
        self.record = record
        # Set tape block size
        self.record.device.count = self.record.mover['record_size']
        
    def readable(self): # Backup
        if (self.record.mover['mode'] == const.NDMP_MOVER_MODE_READ and
            self.record.mover['state'] == const.NDMP_MOVER_STATE_ACTIVE and
            self.record.error == const.NDMP_NO_ERR):
            return True
        
    def writable(self): # Recover
        if (self.record.mover['mode'] == const.NDMP_MOVER_MODE_WRITE and
            self.record.mover['state'] == const.NDMP_MOVER_STATE_ACTIVE and
            self.record.error == const.NDMP_NO_ERR):
            return True

    def handle_read(self): # Backup
        try:
            self.record.device.data = self.recv(int(cfg['BUFSIZE']))
            self.record.device.write(self.record)
            self.record.mover['bytes_moved'] += len(self.record.device.data)
        except (OSError, IOError) as e:
            stdlog.error(self.record.device.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                self.record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                self.record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                self.record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                self.record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.ENOSPC):
                self.record.error = const.NDMP_EOM_ERR
                self.record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_EOM
                stdlog.info('MOVER> pausing status ' + repr(const.ndmp_error[self.record.error]))
                self.record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
                self.record.queue.put(nt.mover_paused().post(self.record))
            else:
                self.record.error = const.NDMP_IO_ERR
        
    def handle_write(self): # Recover
        try:
            self.record.device.read(self.record)
            self.record.mover['bytes_moved'] += self.send(self.record.device.data)
        except (OSError, IOError) as e:
            stdlog.error(self.record.device.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                self.record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                self.record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                self.record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                self.record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.ENOSPC):
                self.record.error = const.NDMP_EOM_ERR
                self.record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_EOM
                stdlog.info('MOVER> pausing status ' + repr(const.ndmp_error[self.record.error]))
                self.record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
                self.record.queue.put(nt.mover_paused().post(self.record))
            else:
                self.record.error = const.NDMP_IO_ERR

    def handle_error(self):
        stdlog.info('MOVER> Connection with ' + repr(self.addr) + ' failed')
        self.record.mover['halt_reason'] = const.NDMP_MOVER_HALT_INTERNAL_ERROR
        self.handle_close() # connection failed, shutdown
        
    def handle_close(self):
        stdlog.info('MOVER> Connection with ' + repr(self.addr) + ' closed')
        self.close()
        self.record.queue.put(nt.mover_halted().post(self.record))
        stdlog.info('MOVER> closing status ' + repr(const.ndmp_error[self.record.error]))
        self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
