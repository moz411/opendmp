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
from xdr import ndmp_const as const
from tools import utils as ut
import tools.mtio
import os

class open():
    '''This request opens the tape device in the specified mode. This
       operation is required before any other tape requests can be executed.'''
    
    async def request_v4(self, record):
        record.tape['path'] = record.b.device.decode()
        if record.tape['path'] not in ut.list_devices():
                stdlog.error('Access to device ' + record.tape['path'] + ' not allowed')
                record.error = const.NDMP_PERMISSION_ERR
                return
        if(record.b.mode == const.NDMP_TAPE_READ_MODE):
            #mode = 'rb'
            mode = os.O_RDONLY|os.O_NONBLOCK
        elif(record.b.mode in [const.NDMP_TAPE_WRITE_MODE,
                               const.NDMP_TAPE_RAW_MODE,
                               const.NDMP_TAPE_RAW2_MODE]):
            #mode = 'wb'
            mode = os.O_WRONLY|os.O_NONBLOCK
        record.tape['fd'] = os.open(record.tape['path'], mode, 0)
        record.tape['opened'] = True
        stdlog.info('TAPE> device ' + record.tape['path'] + ' opened')
    
    async def reply_v4(self, record):
        pass
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class close():
    '''This request closes the tape drive.'''
    
    async def request_v4(self, record):
        pass
    
    async def reply_v4(self, record):
        os.close(record.tape['fd'])
        record.tape['opened'] = False
        stdlog.info('TAPE> device ' + record.tape['path'] + ' closed')
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class get_state():
    '''This request returns the state of the tape drive interface.'''

    async def request_v4(self, record):
        pass
    
    async def reply_v4(self, record):
        record.tape['mt'] = await tools.mtio.MTIOCGET(record)
        stdlog.info('File num: ' + repr(record.tape['mt']['file_num']))
        stdlog.info('Block number: ' + repr(record.tape['mt']['blockno']))
        record.b.unsupported = record.tape['mt']['unsupported']
        record.b.flags = record.tape['mt']['flags']
        record.b.file_num = record.tape['mt']['file_num']
        record.b.soft_errors = record.tape['mt']['soft_errors']
        record.b.block_size = record.tape['mt']['block_size']
        record.b.blockno = record.tape['mt']['blockno']
        record.b.total_space = record.tape['mt']['total_space']
        record.b.space_remain = record.tape['mt']['space_remain']
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class mtio():
    '''This request provides access to common magnetic tape I/O operations.'''
    
    async def request_v4(self, record):
        record.tape['mt'] = await tools.mtio.MTIOCTOP(record)

    async def reply_v4(self, record):
        record.b.resid_count = 0
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class write():
    '''This request writes data to the tape device.'''
    
    async def request_v4(self, record):
        data = record.b.data_out
        record.tape['count'] = os.write(record.tape['fd'],data)

    async def reply_v4(self, record):
        record.b.count = record.tape['count']
    
    request_v3 = request_v4
    reply_v3 = reply_v4

class read():
    '''This request reads data from the tape drive.'''
    async def request_v4(self, record):
        record.tape['count'] = record.b.count
        record.tape['device'].read(record)
        record.b.data_in = record.tape['data']
    
    async def reply_v4(self, record):
        pass
    
    request_v3 = request_v4
    reply_v3 = reply_v4