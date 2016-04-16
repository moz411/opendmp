'''  Notify Interface 
These messages enable the NDMP Server to notify the DMA that 
the NDMP Server requires attention.
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from tools import utils as ut

class connection_status():
    '''This message is sent in response to a connection establishment
        attempt.'''

    @ut.post('ndmp_notify_connected_request',const.NDMP_NOTIFY_CONNECTION_STATUS)
    def post(self, record, shutdown=False):
        record.post_header.sequence = 1
        record.post_header.reply_sequence = 0
        # Body
        record.post_body.protocol_version = const.NDMPV4
        if not shutdown:
            record.post_body.reason = const.NDMP_CONNECTED
            record.post_body.text_reason = 'Connected'
        else:
            record.post_body.reason = const.NDMP_SHUTDOWN
            record.post_body.text_reason = 'Aborted'

class data_halted():
    '''This message is used to notify the DMA that the NDMP Data Server has
        halted.'''

    @ut.post('ndmp_notify_data_halted_request', const.NDMP_NOTIFY_DATA_HALTED)
    def post(self, record):
        record.post_body.reason = record.data['halt_reason']
        record.post_body.text_reason = record.data['text_reason']

class data_read():
    '''This message is used to notify the DMA that the NDMP Server wants to
        read data from a remote Tape Server'''

    @ut.post('ndmp_notify_data_read_request', const.NDMP_NOTIFY_DATA_READ)
    def post(self, record):
        record.post_body.offset = ut.long_long_to_quad(record.data['offset'])
        record.post_body.length = ut.long_long_to_quad(record.data['length'])


class mover_halted():
    '''This message is used to notify the DMA that the NDMP Tape Server has
        entered the halted state.'''
    
    @ut.post('ndmp_notify_mover_halted_request', const.NDMP_NOTIFY_MOVER_HALTED)
    def post(self, record):
        record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        record.post_body.reason = record.mover['halt_reason']
        record.post_body.text_reason = record.mover['text_reason']


class mover_paused():
    '''This message is used to notify the DMA that the NDMP Tape Server has
        paused.'''
    
    @ut.post('ndmp_notify_mover_paused_request', const.NDMP_NOTIFY_MOVER_PAUSED)
    def post(self, record):
        record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
        record.post_body.reason = record.mover['pause_reason']
        record.post_body.seek_position = ut.long_long_to_quad(record.mover['seek_position'])
