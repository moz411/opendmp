import os, errno, threading
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const

class Device():
    path = None
    fd = None
    hctl = None
    opened = False
    sgio = None
    mt = None
    datain_len = None
    mode = os.O_RDWR
    lock = threading.RLock()
    
    def __repr__(self):
        out = []
        out += ['%s' % const.ndmp_tape_open_mode[self.mode]]
        out += ['path=%s' % self.path]
        out += ['opened=%s' % self.opened]
        return '%s' % ', '.join(out)
    __str__ = __repr__    

    def __init__(self, path=None):
        self.path = path
        self.data = None
        
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
            self.opened = True
            record.error = const.NDMP_NO_ERR
            stdlog.info('device ' + self.path + ' opened')
        except (OSError, IOError) as e :
            stdlog.error(self.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 46): # ENOTREADY
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
        
    def close(self, record):
        try:
            os.close(self.fd)
            self.opened = False
            stdlog.info('device ' + self.path + ' closed')
        except (OSError, IOError) as e:
            stdlog.error(self.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == 123): # No medium
                record.error = const.NDMP_NO_TAPE_LOADED_ERR
            else:
                record.error = const.NDMP_IO_ERR
            
    def read(self, record):
        try:
            self.data = os.read(self.fd, self.count)
        except (OSError, IOError) as e :
            stdlog.error(self.path + ': ' + e.strerror)
            if(e.errno == errno.EACCES):
                record.error = const.NDMP_WRITE_PROTECT_ERR
            elif(e.errno == errno.ENOENT):
                record.error = const.NDMP_NO_DEVICE_ERR
            elif(e.errno == errno.EBUSY):
                record.error = const.NDMP_DEVICE_BUSY_ERR
            elif(e.errno == errno.ENODEV):
                record.error = const.NDMP_NO_DEVICE_ERR
            else:
                record.error = const.NDMP_IO_ERR
            raise
            
    def write(self, record):
        count = os.write(self.fd, self.data)
        return count*8*1024 # in bits