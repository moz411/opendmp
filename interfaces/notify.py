'''  Notify Interface 
These messages enable the NDMP Server to notify the DMA that 
the NDMP Server requires attention.
'''
import time
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut
from xdr.ndmp_pack import NDMPPacker

class connection_status():
    '''This message is sent in response to a connection establishment
        attempt.'''

    def post(self, record, shutdown=False):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_NOTIFY_CONNECTION_STATUS
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.sequence = 1
        header.reply_sequence = 0
        header.time_stamp = int(time.time())
        header.error = const.NDMP_NO_ERR
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_notify_connected_request()
        body.protocol_version = const.NDMPV4
        if not shutdown:
            body.reason = const.NDMP_CONNECTED
            body.text_reason = 'Connected'
        else:
            body.reason = const.NDMP_SHUTDOWN
            body.text_reason = 'Aborted'
        p.pack_ndmp_notify_connected_request(body)
        stdlog.debug(body)
        record.queue.put(p.get_buffer())
        

class data_halted():
    '''This message is used to notify the DMA that the NDMP Data Server has
        halted.'''
    
    def post(self, record):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_NOTIFY_DATA_HALTED
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = 0
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_notify_data_halted_request()
        body.reason = record.data['halt_reason']
        body.text_reason = b'\n'.join(x for x in record.data['error'])
        p.pack_ndmp_notify_data_halted_request(body)
        stdlog.debug(body)
        record.queue.put(p.get_buffer())

class data_read():
    '''This message is used to notify the DMA that the NDMP Server wants to
        read data from a remote Tape Server'''

    def post(self, record):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_NOTIFY_DATA_READ
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = 0
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_notify_data_read_request()
        body.offset = ut.long_long_to_quad(record.data['offset'])
        body.length = ut.long_long_to_quad(record.data['length'])
        p.pack_ndmp_notify_data_read_request(body)
        stdlog.debug(body)
        record.queue.put(p.get_buffer())


class mover_halted():
    '''This message is used to notify the DMA that the NDMP Tape Server has
        entered the halted state.'''
    
    def post(self, record):
        p = NDMPPacker()
        record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_NOTIFY_MOVER_HALTED
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = 0
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_notify_mover_halted_request()
        body.reason = record.mover['halt_reason']
        body.text_reason = b'Finished'
        p.pack_ndmp_notify_mover_halted_request(body)
        stdlog.debug(body)
        record.queue.put(p.get_buffer())


class mover_paused():
    '''This message is used to notify the DMA that the NDMP Tape Server has
        paused.'''
    
    def post(self, record):
        p = NDMPPacker()
        record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
        
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_NOTIFY_MOVER_PAUSED
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = 0
        header.sequence = record.server_sequence
        record.server_sequence+=1
        header.time_stamp = int(time.time())
        header.error = record.error
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_notify_mover_paused_request()
        with record.mover['lock']:
            body.reason = record.mover['pause_reason']
            body.seek_position = ut.long_long_to_quad(record.mover['seek_position'])
        p.pack_ndmp_notify_mover_paused_request(body)
        stdlog.debug(body)
        record.queue.put(p.get_buffer())
