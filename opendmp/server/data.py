'''This module create an asyncio Consumer for Data operations 
and process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import asyncio

class DataServer(asyncio.Protocol):
    
    def __init__(self,record):
        self.record = record
            
    def connection_made(self, transport):
        self.transport = transport
        stdlog.info('DATA> Connected to ' + repr(self.transport.get_extra_info('peername')))
        self.transport.set_write_buffer_limits(high=655360, low=65536)
        stdlog.debug('DATA> write_buffer_limits ' + repr(self.transport.get_write_buffer_limits()))
        
    def data_received(self, data):
        pass
            
    def connection_lost(self, exc):
        stdlog.info('DATA> ' + repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        
    def pause_writing(self):
        stdlog.debug(repr(self) + ' pause writing')
        
    def resume_writing(self):
        stdlog.debug(repr(self) + ' resume writing')

        
    def abort(self):
        self.transport.abort()
        