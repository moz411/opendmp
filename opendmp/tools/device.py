from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import tools.utils as ut


class Device():
    
    def __init__(self, record):
        self.record = record
        self.path = self.record.b.device.decode()
        self.fd = None
        self.hctl = None
        self.opened = False
        self.sgio = None
        self.mt = None
        self.datain_len = None
        self.mode = self.record.b.mode
        self.recvd = 0
        self.size = self.record.mover['record_size']
        self.buf = bytearray(self.size*2)
        
    def __repr__(self):
        out = []
        #out += ['%s' % const.ndmp_tape_open_mode[self.mode]]
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
        
        if(self.record.h.message == const.NDMP_TAPE_OPEN):
            self.mode = self.record.b.mode
            if(self.record.b.mode == const.NDMP_TAPE_READ_MODE):
                self.mode = 'rb'
                #self.mode = os.O_RDONLY|os.O_NONBLOCK
            elif(self.record.b.mode in [const.NDMP_TAPE_WRITE_MODE,
                                   const.NDMP_TAPE_RAW_MODE,
                                   const.NDMP_TAPE_RAW2_MODE]):
                self.mode = 'wb'
                #self.mode = os.O_WRONLY|os.O_NONBLOCK
        self.fd = open(self.path, self.mode, self.size)
        self.opened = True
        stdlog.info('device ' + self.path + ' opened')

    @ut.try_io
    def read(self):
        self.data = self.fd.read(self.size)
    
    @ut.try_io
    def write(self, data):
        self.buf.extend(data)
        while len(self.buf) > self.size:
            self.fd.write(self.buf[:self.size])
            self.buf = self.buf[self.size:]
            
    def flush(self):
        while len(self.buf) > self.size:
            self.fd.write(self.buf[self.size:])
            self.buf = self.buf[self.size:]
        fill = bytearray(self.size - len(self.buf))
        self.buf.extend(fill)
        self.fd.write(self.buf)
        self.fd.flush()
        
    @ut.try_io
    def close(self, record):
        self.fd.close()
        self.opened = False
        stdlog.info('device ' + self.path + ' closed')
        stdlog.info('Bytes written: ' + repr(self.recvd))