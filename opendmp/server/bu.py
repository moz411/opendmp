from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import time, asyncio, os, shlex
from interfaces import fh

class Backup_Utility(asyncio.SubprocessProtocol):
    
    
    def __init__(self, record):
        self.record = record
        self.history = []
        self.env = {}
        self.fifo = ut.give_fifo()
        self.file = os.open(self.fifo, os.O_RDONLY|os.O_NONBLOCK)
        self.recvd = 0
        
    def connection_made(self, transport):
        stdlog.debug(repr(self) + ' connection_made')
        self.record.loop.add_reader(self.file, self.move)
        self.stdin = transport.get_pipe_transport(0)

    def connection_lost(self, exc):
        stdlog.debug(repr(self)  + ' connection_lost: ' + repr(exc))

    def pipe_data_received(self, fd, data):
        if fd == 1:
            self.history.append(data)
        elif fd == 2:
            stdlog.error(data.decode())
            
    def pipe_connection_lost(self, fd, exc):
        stdlog.error(repr(self)  + ' pipe_connection_lost%r' % ((fd, exc),))

    def  process_exited(self):
        stdlog.info(repr(self)  + ' process exited')
        ut.clean_fifo(self.fifo)
        # remove reader
        self.record.loop.remove_reader(self.file)
        # close fifo
        os.close(self.file)
        stdlog.info('recvd ' + repr(self.recvd))
        self.record.bu['exit'].set_result(True)
        
    def move(self):
        data = os.read(self.file, 1024)
        self.recvd += len(data)
        self.record.data['server'].transport.write(data)
        
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.dumpdates.update({(self.env['FILESYSTEM'],
                                                   self.env['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))
            
@asyncio.coroutine
def start_bu(record):
    # Launch the BU process in a coroutine
    record.bu['bu'] = record.bu['utility'](record)
    
    # Extract all env variables, overwrite default_env
    for pval in record.bu['bu'].butype_info.default_env:
        name = pval.name.decode().strip()
        value = pval.value.decode().strip()
        record.bu['bu'].env[name] =  value
    for pval in record.bu['env']:
        name = pval.name.decode().strip()
        value = pval.value.decode('utf-8', 'replace').strip()
        record.bu['bu'].env[name] =  value
            
    # Retrieving FILESYSTEM to backup or restore
    try:
        if(record.bu['bu'].env['FILES']):
            record.bu['bu'].env['FILESYSTEM'] = record.bu['bu'].env['FILES']
        assert(record.bu['bu'].env['FILESYSTEM'] != None)
        assert(os.path.exists(record.bu['bu'].env['FILESYSTEM']))
    except KeyError:
        pass
    except AssertionError:
        stdlog.error('FILESYSTEM ' + record.bu['bu'].env['FILESYSTEM'] + ' does not exists')
        record.error = const.NDMP_ILLEGAL_ARGS_ERR
        return

    stdlog.info('Starting backup of ' + record.bu['bu'].env['FILESYSTEM'])
    
        
    record.bu['exit'] = asyncio.Future(loop=record.loop)

    # Create the subprocess controlled by the protocol Backup_Utility,
    # redirect the standard output into a pipe
    
    
    #create = record.loop.subprocess_exec(lambda: record.bu['bu'],
    #                                     '/bin/tar','-cPvf',
    #                                     filename,record.bu['bu'].env['FILESYSTEM'])
    create = record.loop.subprocess_exec(lambda: record.bu['bu'],
                                         '/bin/cp','/usr/share/mythes/th_en_US_v2.dat',record.bu['bu'].fifo)
    transport, protocol = yield from create
    
    # Wait for the subprocess exit using the process_exited() method
    # of the protocol
    yield from record.bu['exit']

    # Close the stdout pipe
    transport.close()

    # Read the output which was collected by the pipe_data_received()
    # method of the protocol
    stdlog.info(protocol.history)

    