'''This module create an asyncio Consumer for Mover operations
and process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify
import asyncio, os

class MoverServer(asyncio.Protocol):
    
    def __init__(self, record):
        self.record = record
        self.recvd = 0

    def connection_made(self, transport):
        self.transport = transport
        stdlog.info('MOVER> Connected to ' + repr(self.transport.get_extra_info('peername')))
        self.record.mover['state'] = const.NDMP_MOVER_STATE_ACTIVE
        self.record.mover['server'].close()
        
    def data_received(self, data):
        iter = 0
        self.recvd += len(data)
        self.record.mover['buffer'].extend(data)
        while len(self.record.mover['buffer']) > self.record.mover['record_size']:
            print('iter ' + repr(iter) + ': ' + repr(len(self.record.mover['buffer']))) 
            self.record.mover['bytes_moved'] += os.writev(self.record.tape['fd'],
                                [self.record.mover['buffer'][:self.record.mover['record_size']]])
            self.record.mover['buffer'] = self.record.mover['buffer'][self.record.mover['record_size']:]
            iter+=1
            
    def connection_lost(self, exc):
        stdlog.info('MOVER>' + repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        self.record.mover['bytes_moved'] += os.writev(self.record.tape['fd'],
                                                    [self.record.mover['buffer']])
        self.record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
        asyncio.ensure_future(notify.mover_halted().post(self.record))
        
    def pause_writing(self):
        stdlog.debug(repr(self) + ' pause writing')
        
    def resume_writing(self):
        stdlog.debug(repr(self) + ' resume writing')

    def abort(self):
        pass