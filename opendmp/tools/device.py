import os
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

        self.fd = os.open(self.path, mode)
        self.opened = True
        stdlog.info('device ' + self.path + ' opened')
        
    def close(self, record):
        os.close(self.fd)
        self.opened = False
        stdlog.info('device ' + self.path + ' closed')
            
    def read(self, record):
        self.data = os.read(self.fd, self.count)
            
    def write(self, record):
        self.count = os.write(self.fd, self.data)
        return self.count*8*1024 # in bits