'''This interface is used by the NDMP Server to send informational and
diagnostic data to the DMA. This data is used by the client to
monitor the progress of the currently running data operation and to
diagnose problems.'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from tools import utils as ut
from logging.handlers import QueueHandler
from logging import Handler


class message():
    '''This post sends an informational message to the DMA'''
    
    @ut.post('ndmp_log_message_request_v4',const.NDMP_LOG_MESSAGE)
    async def post(self, record):
        if record.data['bu'].retcode == 0:
            record.post_body.log_type = const.NDMP_LOG_NORMAL
        else:
            record.post_body.log_type = const.NDMP_LOG_ERROR
        record.post_body.message_id = 0
        record.post_body.entry = repr(b'\n'.join(record.data['bu'].errmsg)).encode()
        record.post_body.associated_message_valid = const.NDMP_NO_ASSOCIATED_MESSAGE
        record.post_body.associated_message_sequence = 0
        
    
class file(QueueHandler):
    '''This post sends a file recovered message to the DMA'''
    
    async def __init__(self, record):
        Handler.__init__(self)
        self.queue = record.queue
    
    @ut.post('ndmp_log_file_request_v4',const.NDMP_MESSAGE_POST)
    async def post(self, record):
        record.post_body.name = record.data['file']
        record.post_body.recovery_status = record.data['recovery']['status']
        
    emit = post