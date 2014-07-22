''' Tape interface 
This interface supports tape positioning and tape read/write 
operations. The DMA typically uses the Tape interface to write 
tape metadata. This includes tape labels and information 
identifying and describing backup data included on the tape. The 
DMA also uses the Tape interface to position the tape during 
backups and recoveries.
'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from tools import utils as ut
from tools.device import Device
import tools.mtio

class open():
    '''This request opens the tape device in the specified mode. This
       operation is required before any other tape requests can be executed.'''
    def request_v4(self, record):
        if(ut.check_device_not_opened(record)):  return
        record.device = Device(path=record.b.device.decode())
        record.device.open(record)
    
    def reply_v4(self, record):
        pass
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class close():
    '''This request closes the tape drive.'''
    def request_v4(self, record):
        pass
    
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): return
        record.device.close(record)

    request_v3 = request_v4
    reply_v3 = reply_v4

class get_state():
    '''This request returns the state of the tape drive interface.'''

    def reply_v4(self, record):
        if ut.check_device_opened(record):
            mt = tools.mtio.MTIOCGET(record)
            record.b.unsupported = mt['unsupported']
            record.b.flags = mt['flags']
            record.b.file_num = mt['file_num']
            record.b.soft_errors = mt['soft_errors']
            record.b.block_size = mt['block_size']
            record.b.blockno = mt['blockno']
            record.b.total_space = mt['total_space']
            record.b.space_remain = mt['space_remain']
            stdlog.info('File num: ' + repr(mt['file_num']))
            stdlog.info('Block number: ' + repr(mt['blockno']))
        else:
            record.b.unsupported = 0
            record.b.flags = 0
            record.b.file_num = 0
            record.b.soft_errors = 0
            record.b.block_size = 0
            record.b.blockno = 0
            record.b.invalid = 0
            record.b.partition = 0
            record.b.total_space = ut.long_long_to_quad(0)
            record.b.space_remain = ut.long_long_to_quad(0)
    
    reply_v3 = reply_v4

class mtio():
    '''This request provides access to common magnetic tape I/O operations.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        tools.mtio.MTIOCTOP(record)
            
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): 
            record.b.resid_count = 0
        else:
            mt = tools.mtio.MTIOCGET(record)
            record.b.resid_count = mt['resid']

    request_v3 = request_v4
    reply_v3 = reply_v4

class write():
    '''This request writes data to the tape device.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): 
            return
        else:
            record.device.data = record.b.data_out
            record.device.write(record)

    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): 
            record.b.count = 0
        else:
            record.b.count = record.device.count

    request_v3 = request_v4
    reply_v3 = reply_v4

class read():
    '''This request reads data from the tape drive.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        record.device.count = record.b.count
        record.device.read(record)
    
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): 
            record.b.data_in = 0
        else:
            record.b.data_in = record.device.data

    request_v3 = request_v4
    reply_v3 = reply_v4
