'''This module create a asyncore Consumer for Mover operations
and process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify
import asyncio

class MoverServer(asyncio.Protocol):
    
    def __init__(self, record):
        self.record = record

    def connection_made(self, transport):
        self.transport = transport
        stdlog.info('MOVER> Connected to ' + repr(self.transport.get_extra_info('peername')))
        self.record.mover['state'] = const.NDMP_MOVER_STATE_ACTIVE
        self.record.mover['server'].close()
        
    def data_received(self, data):
        self.record.device.write(data)
        self.record.mover['bytes_moved'] += len(data)
            
    def connection_lost(self, exc):
        stdlog.info('MOVER>' + repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        self.record.device.flush()
        self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        asyncio.ensure_future(notify.mover_halted().post(self.record))
        
    def pause_writing(self):
        stdlog.debug(repr(self) + ' pause writing')
        
    def resume_writing(self):
        stdlog.debug(repr(self) + ' resume writing')

    def abort(self):
        pass