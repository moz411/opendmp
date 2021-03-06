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
        self.bufsize = int(cfg['BUFSIZE'])
        self.recvd = 0
        self.retcode = None
        self.file = os.open(self.fifo, os.O_RDONLY|os.O_NONBLOCK)
        self.file2 = os.fdopen(self.file, 'rb')
        
    def connection_made(self, transport):
        self.transport = transport
        stdlog.debug(repr(self) + ' connection_made')
        self.record.loop.add_reader(self.file,self.copy_data)

    def connection_lost(self, exc):
        stdlog.debug(repr(self)  + ' connection_lost: ' + repr(exc))
        pass

    def pipe_data_received(self, fd, data):
        if fd == 1:
            for line in data.decode().splitlines():
                try:
                    entry = self.add_file(line)
                    self.history.append(entry)
                except ValueError as e:
                    stdlog.error(line)
                    stdlog.error(e)
            
            if(len(self.history) > int(cfg['FH_MAXLINES'])):
                asyncio.ensure_future(fh.add_file().post(self.record))
                
        elif fd == 2:
            self.error.append(data.decode())
            
    def pipe_connection_lost(self, fd, exc):
        #stdlog.error(repr(self)  + ' pipe_connection_lost%r' % ((fd, exc),))
        #self.record.data['server'].transport.drain()
        pass
        
    def  process_exited(self):
        stdlog.debug(repr(self)  + ' process exited')
        # get retcode
        self.retcode = self.transport.get_returncode()
        asyncio.ensure_future(fh.add_file().post(self.record))
        
        # close socket to Mover
        self.record.data['server'].transport.close()
        
        # remove reader
        self.record.loop.remove_reader(self.file)
        # close fifo
        os.close(self.file)
        # remove the fifo
        ut.clean_fifo(self.fifo)
        
        # alert the DMA of halt
        self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
        if self.retcode == 0:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
        else:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            self.record.data['text_reason'] = b''.join(repr(x).encode() for x in self.error)
            
        asyncio.ensure_future(notify.data_halted().post(self.record))
        
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.dumpdates.update({(self.env['FILESYSTEM'], self.env['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))
            
    def copy_data(self):
        self.record.data['bytes_moved'] += self.file2.readinto(self.record.data['buffer'])
        self.record.data['server'].transport.write(self.record.data['buffer'])
        
    def pause_writing(self):
        stdlog.debug(repr(self) + ' pause writing')
        
    def resume_writing(self):
        stdlog.debug(repr(self) + ' resume writing')

