from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify
import time, asyncio, os
from interfaces import fh

class Backup_Utility(asyncio.SubprocessProtocol):
    
    
    def __init__(self, record):
        self.record = record
        self.history = []
        self.error = []
        self.env = {}
        self.fifo = ut.give_fifo()
        self.recvd = 0
        self.retcode = None
        self.file = os.open(self.fifo, os.O_RDONLY|os.O_NONBLOCK)
        
    def connection_made(self, transport):
        self.transport = transport
        stdlog.debug(repr(self) + ' connection_made')
        self.record.loop.add_reader(self.file,self.move_data)

    def connection_lost(self, exc):
        #stdlog.debug(repr(self)  + ' connection_lost: ' + repr(exc))
        pass

    def pipe_data_received(self, fd, data):
        if fd == 1:
            for line in data.decode().splitlines():
                try:
                    entry = yield from self.add_file(line)
                    self.history.append(entry)
                except ValueError as e:
                    stdlog.error(line)
                    stdlog.error(e)
            
            if(len(self.history) > int(cfg['FH_MAXLINES'])):
                asyncio.ensure_future(fh.add_file().post(self.record))
                self.history.clear()
                
        elif fd == 2:
            self.error.append(data.decode())
            
    def pipe_connection_lost(self, fd, exc):
        #stdlog.error(repr(self)  + ' pipe_connection_lost%r' % ((fd, exc),))
        yield from self.record.data['server'].transport.drain()
        
    def  process_exited(self):
        stdlog.debug(repr(self)  + ' process exited')
        ut.clean_fifo(self.fifo)
        # remove reader
        self.record.loop.remove_reader(self.file)
        # close fifo
        os.close(self.file)
        # get retcode
        self.retcode = self.transport.get_returncode()
        asyncio.ensure_future(fh.add_file().post(self.record))
        self.history.clear()
        
        self.record.data['server'].transport.close()
        # alert the DMA of halt
        self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
        if self.retcode == 0:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
        else:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            self.record.data['text_reason'] = b'\n'.join(repr(x).encode() for x in self.error)
            
        asyncio.ensure_future(notify.data_halted().post(self.record))
        self.transport.close()
        
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.dumpdates.update({(self.env['FILESYSTEM'],
                                                   self.env['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))
            
    def move_data(self):
        data = os.read(self.file, self.record.bufsize)
        self.record.data['server'].transport.write(data)
        self.record.data['bytes_moved'] += len(data)
        
    def pause_writing(self):
        stdlog.debug(repr(self) + ' pause writing')
        
    def resume_writing(self):
        stdlog.debug(repr(self) + ' resume writing')


