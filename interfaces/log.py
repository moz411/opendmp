'''This interface is used by the NDMP Server to send informational and
diagnostic data to the DMA. This data is used by the client to
monitor the progress of the currently running data operation and to
diagnose problems.'''
               
import time
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut
from xdr.ndmp_pack import NDMPPacker

class message():
    '''This post sends an informational message to the DMA'''
    
    def post(self, record):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        record.reset()
        header.message = const.NDMP_MESSAGE_POST
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = record.dma_sequence
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_log_message_request_v4()
        body.log_type = record.log['type']
        body.message_id = record.log['id']
        body.entry = record.log['entry']
        body.associated_message_valid = const.NDMP_HAS_ASSOCIATED_MESSAGE
        body.associated_message_sequence = record.server_sequence-1
        p.pack_ndmp_log_message_request_v4(body)
        return p.get_buffer()
    
class file():
    '''This post sends a file recovered message to the DMA'''
    
    def post(self, record):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_MESSAGE_POST
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = record.dma_sequence
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_log_file_request_v4
        body.name = record.data['file']
        body.recovery_status = record.data['recovery']['status']
        p.pack_ndmp_log_file_request_v4(body)
        return p.get_buffer()
        
