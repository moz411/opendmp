import os, errno, traceback
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const

class Device():
    
    def __init__(self, path=None):
        self.path = path
        self.data = None
        self.fd = None
        self.hctl = None
        self.opened = False
        self.sgio = None
        self.mt = None
        self.datain_len = None
        self.mode = os.O_RDWR
        self.count = 0
        
    def __repr__(self):
        out = []
        out += ['%s' % const.ndmp_tape_open_mode[self.mode]]
        out += ['path=%s' % self.path]
        out += ['opened=%s' % self.opened]
        return '%s' % ', '.join(out)
    __str__ = __repr__    
        
    def open(self, record):
        if(record.h.message == const.NDMP_TAPE_OPEN):
            self.mode = record.b.mode
            if(record.b.mode == const.NDMP_TAPE_READ_MODE):
                mode = os.O_RDONLY
            elif(record.b.mode == const.NDMP_TAPE_WRITE_MODE):
                mode = os.O_RDWR | os.O_NDELAY
            elif(record.b.mode in [const.NDMP_TAPE_RAW_MODE,
                                   const.NDMP_TAPE_RAW2_MODE]):
                # mode = os.O_DIRECT
                # TODO: fix O_DIRECT mode that did not work on Linux 2.6.38
                mode = os.O_RDWR | os.O_NDELAY
        else:
            mode = os.O_RDWR | os.O_NDELAY
        try:
            self.fd = os.open(self.path, mode)
        except (OSError, IOError) as e:
            stdlog.error(record.device.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 123):
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
        else:
            self.opened = True
            stdlog.info('device ' + self.path + ' opened')
        
    def close(self, record):
        try:
            os.close(self.fd)
        except (OSError, IOError) as e:
            stdlog.error(record.device.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 123):
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
        else:
            self.opened = False
            stdlog.info('device ' + self.path + ' closed')
            
    def read(self, record):
        try:
            self.data = os.read(self.fd, self.count)
        except (OSError, IOError) as e:
            if(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 123):
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
            
    def write(self, record):
        try:
            self.count = os.write(self.fd, self.data)
        except (OSError, IOError) as e:
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 123):
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
        else:
            return self.count*8*1024 # in bits