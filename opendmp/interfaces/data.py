''' Data Interface 
This interface initiates backup and recover operations. The DMA 
provides all the parameters that affect the backup or recovery 
using the Data Interface. The DMA does not place any constraints 
on the format of the backup data other than it MUST be a stream 
of data that can be written to the tape device. 
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import asyncio, re, shlex
from server.data import DataServer
from interfaces import notify
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut, ipaddress as ip, plugins


#sys.path.append("../tools/pysendfile-2.0.1-py3.4-linux-x86_64.egg")
#print(sys.path)
#import sendfile
#print(sendfile)

class connect():
    '''This request is used by the DMA to instruct the Data Server to
        establish a data connection to a Tape Server or peer Data Server'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_IDLE)
    async def request_v4(self, record):
        self.record = record
        record.data['addr_type'] = record.b.addr_type
        if record.data['addr_type'] == const.NDMP_ADDR_TCP:
            record.data['addr_type'] = record.b.addr_type
            ip_int = record.b.tcp_addr[0].ip_addr
            record.data['host'] = ip.IPv4Address(ip_int)._string_from_ip_int(ip_int)
            record.data['port'] = self.record.b.tcp_addr[0].port
            record.data['server'] = DataServer(record)
            await record.loop.create_connection(lambda: record.data['server'],
                                          record.data['host'],
                                          record.data['port'])
            record.data['state'] = const.NDMP_DATA_STATE_CONNECTED
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR
            
    async def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4


class listen():
    '''This request is used by the DMA to instruct the Data Server create a
       connection end point and listen for a subsequent data connection from
       a Tape Server (mover) or peer Data Server. This request is also used
       by the DMA to obtain the address of connection end point the Data
       Server is listening at.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_IDLE)
    async def request_v4(self, record):
        self.record = record
        record.data['addr_type'] = record.b.addr_type
        if record.data['addr_type'] == const.NDMP_ADDR_TCP:
            fd = ip.get_next_data_conn()
            record.data['host'], record.data['port'] = fd.getsockname()
            fd.close()
            record.data['server'] = await record.loop.create_server(DataServer, 
                                                       record.data['host'], 
                                                       record.data['port'])
            record.data['state'] = const.NDMP_DATA_STATE_LISTEN
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR
        
    @ut.valid_state(const.NDMP_DATA_STATE_LISTEN)
    async def reply_v4(self, record):
        record.b.connect_addr = type.ndmp_addr_v4()
        record.b.connect_addr.addr_type = record.data['addr_type']
        if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.connect_addr.tcp_addr = []
            tcp_addr = type.ndmp_tcp_addr_v4(record.data['server'].host,
                                             record.data['server'].port,[])
            record.b.connect_addr.tcp_addr.append(tcp_addr)
            record.data['peer'] = tcp_addr
    
    @ut.valid_state(const.NDMP_DATA_STATE_LISTEN)
    async def reply_v3(self, record):
        record.b.connect_addr = type.ndmp_addr_v4()
        record.b.connect_addr.addr_type = record.data['addr_type']
        if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.data_connection_addr = type.ndmp_addr_v3
            record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.data_connection_addr.tcp_addr = type.ndmp_tcp_addr
            record.b.data_connection_addr.tcp_addr.ip_addr = record.data['host']
            record.b.data_connection_addr.tcp_addr.port = record.data['port']

    request_v3 = request_v4

class start_backup(asyncio.SubprocessProtocol):
    '''This request is used by the DMA to instruct the Data Server to
       initiate a backup operation and begin transferring backup data from
       the file system represented by this Data Server to a Tape Server or
       peer Data Server over the previously established data connection.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_CONNECTED)
    @plugins.validate
    async def request_v4(self, record):
        record.bu['bu'] = record.bu['utility'](record)
        record.data['operation'] = const.NDMP_DATA_OP_BACKUP
        
        # Extract all env variables, overwrite default_env
        for pval in record.bu['bu'].butype_info.default_env:
            name = pval.name.decode().strip()
            value = pval.value.decode().strip()
            record.bu['bu'].env[name] =  value
        for pval in record.b.env:
            name = pval.name.decode().strip()
            value = pval.value.decode('utf-8', 'replace').strip()
            record.bu['bu'].env[name] =  value
                
        # Retrieving FILESYSTEM to backup or restore
        if(record.bu['bu'].env['FILES']):
            record.bu['bu'].env['FILESYSTEM'] = record.bu['bu'].env['FILES']

        executable = record.bu['bu'].executable
        args = record.bu['bu'].args
        
        args = re.sub('FIFO', record.bu['bu'].fifo, args)
        args = re.sub('FILESYSTEM', record.bu['bu'].env['FILESYSTEM'], args)
        args = shlex.split(executable + ' ' + args)
        
        (transport,_) = await record.loop.subprocess_exec(lambda: record.bu['bu'], 
                                       stdout=asyncio.subprocess.PIPE,
                                       stderr=asyncio.subprocess.PIPE,
                                       *args)
        
        stdlog.info('Starting backup of ' + record.bu['bu'].env['FILESYSTEM'])
        
    async def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4

class start_recover():
    '''This request is used by the DMA to instruct the Data Server to
       initiate a recovery operation and transfer the recovery stream
       received from a Tape Server or peer Data Server over the previously
       established data connection to the specified local file system
       location.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_CONNECTED)
    @plugins.validate
    async def request_v4(self, record):
        pass
    
    async def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4
    
class start_recover_filehist():
    '''This optional request is used by the DMA to instruct the Data Server
       to initiate a file history recovery operation and process the
       recovery stream received from a Tape Server or peer Data Server over
       the previously established data connection to generate file history
       as during backup operations. No changes are made to the local file
       system.'''
    
    async def request_v4(self, record):
        pass
    
    async def reply_v4(self, record):
        record.error = const.NDMP_NOT_SUPPORTED_ERR
    
    request_v3 = request_v4
    reply_v3 = reply_v4
    

class get_state():
    '''This request is used by the DMA to obtain information about the Data
       Server's operational state as represented by the Data Server variable
       set.'''
    
    async def reply_v4(self, record):
        bytes_moved = record.data['bytes_moved']
        record.b.state = record.data['state']
        record.b.unsupported = (const.NDMP_DATA_STATE_EST_TIME_REMAIN_INVALID |
                                const.NDMP_DATA_STATE_EST_BYTES_REMAIN_INVALID)
        record.b.operation = record.data['operation']
        record.b.halt_reason = record.data['halt_reason']
        record.b.bytes_processed = ut.long_long_to_quad(bytes_moved)
        # record.b.est_bytes_remain = ut.long_long_to_quad(remain)
        record.b.est_bytes_remain = ut.long_long_to_quad(0)
        record.b.est_time_remain = 0
        record.b.invalid = 0
        try:
            if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
                intaddr = ip.ip_address(record.data['host'])
                addr = intaddr._ip_int_from_string(record.data['host'])
                record.b.data_connection_addr = type.ndmp_addr_v4(const.NDMP_ADDR_TCP,
                                                    [type.ndmp_tcp_addr_v4(addr, record.data['port'],
                                                    [type.ndmp_pval(name=b'', value=b'')])])
            elif(record.data['addr_type'] == const.NDMP_ADDR_IPC):
                record.b.data_connection_addr = type.ndmp_ipc_addr(b'')
            else:
                record.b.data_connection_addr = type.ndmp_addr_v4(const.NDMP_ADDR_LOCAL)
        except ValueError:
            record.b.data_connection_addr = type.ndmp_addr_v4(const.NDMP_ADDR_LOCAL)
        record.b.read_offset = ut.long_long_to_quad(0)
        record.b.read_length = ut.long_long_to_quad(0)
            
        stdlog.info('Bytes processed: ' + repr(bytes_moved))

    reply_v3 = reply_v4
    
    

class get_env():
    '''This request is used by the DMA to obtain the backup environment
       variable set associated with the current data operation. The
       NDMP_DATA_GET_ENV request is typically issued following a successful
       backup operation but MAY be issued during or after a recovery
       operation as well.'''
    
    async def reply_v4(self, record):
        record.b.env = []
        for var in record.bu['bu'].env:
            record.b.env.append(type.ndmp_pval(name=repr(var).encode(), 
                                               value=repr(record.bu['bu'].env[var]).encode()))

    reply_v3 = reply_v4

class stop():
    '''This request is used by the DMA to instruct the Data Server to
       release all resources, reset all Data Server state variables, reset
       all backup environment variables and transition the Data Server to
       the IDLE state.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_HALTED)
    async def reply_v4(self, record):
        record.data['halt_reason'] = const.NDMP_DATA_HALT_NA
        record.data['state'] = const.NDMP_DATA_STATE_IDLE
        record.data['operation'] = const.NDMP_DATA_OP_NOACTION
        record.data['bytes_moved'] = 0
                
    reply_v3 = reply_v4

class abort():
    '''This request is used by the DMA to instruct the Data Server to
       terminate any in progress data operation, close the data connection
       if present, and transition the Data Server to the HALTED state.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_IDLE, False)
    async def reply_v4(self, record):
        try:
            record.bu['bu'].kill()
        except AttributeError: # No BU defined
            pass
        record.data['server'].abort()
        record.data['halt_reason'] = const.NDMP_DATA_HALT_ABORTED
        record.data['state'] = const.NDMP_DATA_STATE_HALTED
        record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            
    reply_v3 = reply_v4