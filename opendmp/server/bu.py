from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify
import time, asyncio, os, re, shlex
from interfaces import fh

class Backup_Utility(asyncio.SubprocessProtocol):
    
    
    def __init__(self, record):
        self.record = record
        self.history = []
        self.error = []
        self.env = {}
        self.fifo = ut.give_fifo()
        self.recvd = 0
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
            for line in data.decode().split('\n'):
                try:
                    if (len(line) > 0) : self.history.append(self.add_file(line))
                except ValueError as e:
                    print(line)
                    print(e)
            
            if(len(self.history) > 10):
                fh.add_file().post(self.record)
                self.history.clear()
        elif fd == 2:
            self.error.append(data.decode())
            
    def pipe_connection_lost(self, fd, exc):
        #stdlog.error(repr(self)  + ' pipe_connection_lost%r' % ((fd, exc),))
        pass
        
    def  process_exited(self):
        stdlog.debug(repr(self)  + ' process exited')
        ut.clean_fifo(self.fifo)
        # remove reader
        self.record.loop.remove_reader(self.file)
        # close fifo
        os.close(self.file)
        # get retcode
        #retcode = self.get_returncode()
        #print('retcode: ' + repr(retcode))
        retcode = 0
        self.record.data['server'].transport.close()
        # alert the DMA of halt
        self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
        if retcode == 0:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
        else:
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            self.record.data['text_reason'] = b'\n'.join(repr(x).encode() for x in self.error)
        notify.data_halted().post(self.record)
        self.record.bu['exit'].set_result(True)
        
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
    
    executable = record.bu['bu'].executable
    args = record.bu['bu'].args
    
    args = re.sub('FIFO', record.bu['bu'].fifo, args)
    args = re.sub('FILESYSTEM', record.bu['bu'].env['FILESYSTEM'], args)
    args = shlex.split(executable + ' ' + args)
        
    try:
        create = record.loop.subprocess_exec(lambda: record.bu['bu'], *args)
        transport, protocol = yield from create
    except Exception as e:
        stdlog.error(e)
        record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
        record.data['state'] = const.NDMP_DATA_STATE_HALTED
    else:
        record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
    # Wait for the subprocess exit using the process_exited() method
    # of the protocol
    yield from record.bu['exit']

    # Close the stdout pipe
    transport.close()

    # Read the output which was collected by the pipe_data_received()
    # method of the protocol
    #stdlog.info(protocol.history)
