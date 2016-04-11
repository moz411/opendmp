'''This module create a asyncore Consumer for Mover operations
and process the data stream'''

from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import asyncio

class MoverServer(asyncio.Protocol):
    
    def __init__(self, record):
        self.record = record
            
    def connection_made(self, transport):
        self.transport = transport
        stdlog.info('MOVER> Connected to ' + repr(self.transport.get_extra_info('peername')))
        self.record.mover['state'] = const.NDMP_MOVER_STATE_ACTIVE
        
    def data_received(self, data):
        pass
            
    def connection_lost(self, exc):
        stdlog.info('MOVER>' + repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        self.record.close()
        self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED

    def abort(self):
        self.transport.abort()
        
        