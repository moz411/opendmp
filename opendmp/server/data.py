'''This module create a asyncore Consumer for Data operations 
and process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import asyncio

class DataServer(asyncio.Protocol):
    
    def __init__(self,record):
        self.record = record
            
    def connection_made(self, transport):
        self.transport = transport
        self.record.data['server'] = self
        stdlog.info('DATA> Connected to ' + repr(self.transport.get_extra_info('peername')))
        
    def data_received(self, data):
        pass
            
    def connection_lost(self, exc):
        stdlog.info('DATA>' + repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        self.record.close()
        
        
    def abort(self):
        self.transport.abort()