import os, io
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const

class Device():
    
    def __init__(self, path=None):
        self.path = path
        self.data = b''
        self.writer = None
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
        if(self.mode == const.NDMP_TAPE_READ_MODE):
            self.fd = os.open(self.path, os.O_RDONLY)
        elif(self.mode in [const.NDMP_TAPE_WRITE_MODE,
                               const.NDMP_TAPE_RAW_MODE,
                               const.NDMP_TAPE_RAW2_MODE]):
            # mode = os.O_DIRECT
            # TODO: fix O_DIRECT mode that did not work on Linux 2.6.38
            # mode = os.O_RDWR | os.O_NDELAY
            raw = open(self.path, 'wb')
            self.writer = io.BufferedWriter(raw, record.mover['record_size'])
            self.fd = self.writer.fileno()
        self.opened = True
        stdlog.info('device ' + self.path + ' opened')
        
    def close(self, record):
        os.close(self.fd)
        self.opened = False
        stdlog.info('device ' + self.path + ' closed')
            
    def read(self, record):
        self.data = os.read(self.fd, self.count)
            
    def write(self, record):
        written = self.writer.write(self.data)
        return written*8*1024 # in bits