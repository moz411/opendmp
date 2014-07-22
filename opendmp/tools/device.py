import os, errno
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import tools.utils as ut

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
        
    def __repr__(self):
        out = []
        out += ['%s' % const.ndmp_tape_open_mode[self.mode]]
        out += ['path=%s' % self.path]
        out += ['opened=%s' % self.opened]
        return '%s' % ', '.join(out)
    __str__ = __repr__    
      
    @ut.try_io    
    def open(self, record):
        # List local drives and changers
        # device.open will only allow these devices
        if self.path not in ut.list_devices():
            stdlog.error('Access to device ' + self.path + ' not allowed')
            record.error = const.NDMP_PERMISSION_ERR
            return
        
        if(record.h.message == const.NDMP_TAPE_OPEN):
            self.mode = record.b.mode
            if(record.b.mode == const.NDMP_TAPE_READ_MODE):
                mode = 'rb'
            elif(record.b.mode in [const.NDMP_TAPE_WRITE_MODE,
                                   const.NDMP_TAPE_RAW_MODE,
                                   const.NDMP_TAPE_RAW2_MODE]):
                mode = 'wb'
        else:
            mode = os.O_RDWR | os.O_NDELAY
        self.fd = open(self.path, mode, record.mover['record_size'])
        self.opened = True
        stdlog.info('device ' + self.path + ' opened')
    
    @ut.try_io
    @ut.device_opened
    def close(self, record):
        self.fd.close()
        self.opened = False
        stdlog.info('device ' + self.path + ' closed')

    @ut.try_io
    @ut.device_opened
    def read(self, record):
        self.data = os.read(self.fd, record.mover['record_size'])
    
    @ut.try_io
    @ut.device_opened
    def write(self, record):
        count = self.fd.write(self.data)
        record.mover['bytes_moved'] += count