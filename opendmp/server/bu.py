from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt, log as ndmplog, fh
import time, asyncio

class Backup_Utility(asyncio.SubprocessProtocol):
        
    def connection_made(self, transport):
        self.stdin = transport.get_pipe_transport(0)

    def connection_lost(self, exc):
        stdlog.error(exc)
        
    def pipe_data_received(self, fd, data):
        print('pipe_data_received%r' % ((fd, data),))
        
    def pipe_connection_lost(self, fd, exc):
        print('pipe_connection_lost%r' % ((fd, exc),))
        
    def  process_exited(self):
        stdlog.info('process exited')
                  
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.dumpdates.update({(self.env['FILESYSTEM'],
                                                   self.env['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))