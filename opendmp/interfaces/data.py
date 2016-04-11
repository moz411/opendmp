''' Data Interface 
This interface initiates backup and recover operations. The DMA 
provides all the parameters that affect the backup or recovery 
using the Data Interface. The DMA does not place any constraints 
on the format of the backup data other than it MUST be a stream 
of data that can be written to the tape device. 
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import sys, subprocess, shlex, socket, os, asyncio, traceback, functools
from server.data import DataServer
from server.bu import start_bu
from server.fh import Fh
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
    def request_v4(self, record):
        self.record = record
        record.data['addr_type'] = record.b.addr_type
        if record.data['addr_type'] == const.NDMP_ADDR_TCP:
            record.data['addr_type'] = record.b.addr_type
            ip_int = record.b.tcp_addr[0].ip_addr
            record.data['host'] = ip.IPv4Address(ip_int)._string_from_ip_int(ip_int)
            record.data['port'] = self.record.b.tcp_addr[0].port
            record.data['server'] = record.loop.create_connection(lambda: DataServer(record),
                                          record.data['host'],
                                          record.data['port'])
            asyncio.wait_for(asyncio.async(record.data['server']),None)
            record.data['state'] = const.NDMP_DATA_STATE_CONNECTED
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR
            
    def reply_v4(self, record):
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
    def request_v4(self, record):
        self.record = record
        record.data['addr_type'] = record.b.addr_type
        if record.data['addr_type'] == const.NDMP_ADDR_TCP:
            fd = ip.get_next_data_conn()
            record.data['host'], record.data['port'] = fd.getsockname()
            fd.close()
            record.data['server'] = record.loop.create_server(DataServer, 
                                                       record.data['host'], 
                                                       record.data['port'])
            asyncio.wait_for(asyncio.async(record.data['server']),None)
            record.data['state'] = const.NDMP_DATA_STATE_LISTEN
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR
        
    @ut.valid_state(const.NDMP_DATA_STATE_LISTEN)
    def reply_v4(self, record):
        record.b.connect_addr = type.ndmp_addr_v4()
        record.b.connect_addr.addr_type = record.data['addr_type']
        if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.connect_addr.tcp_addr = []
            tcp_addr = type.ndmp_tcp_addr_v4(record.data['server'].host,
                                             record.data['server'].port,[])
            record.b.connect_addr.tcp_addr.append(tcp_addr)
            record.data['peer'] = tcp_addr
    
    @ut.valid_state(const.NDMP_DATA_STATE_LISTEN)
    def reply_v3(self, record):
        record.b.connect_addr = type.ndmp_addr_v4()
        record.b.connect_addr.addr_type = record.data['addr_type']
        if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.data_connection_addr = type.ndmp_addr_v3
            record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.data_connection_addr.tcp_addr = type.ndmp_tcp_addr
            record.b.data_connection_addr.tcp_addr.ip_addr = record.data['host']
            record.b.data_connection_addr.tcp_addr.port = record.data['port']

    request_v3 = request_v4

class start_backup():
    '''This request is used by the DMA to instruct the Data Server to
       initiate a backup operation and begin transferring backup data from
       the file system represented by this Data Server to a Tape Server or
       peer Data Server over the previously established data connection.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_CONNECTED)
    @plugins.validate
    def request_v4(self, record, *args, **kwargs):
        record.data['operation'] = const.NDMP_DATA_OP_BACKUP
        record.bu['env'] = record.b.env
        # Start the backup and release the Data Consumer
        asyncio.async(start_bu(record))
        record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
        
        # Launch the File History Consumer
        #record.data['fh'] = Fh(record)
        #record.data['fh'].start()
        
    def reply_v4(self, record):
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
    def request_v4(self, record):
        record.data['operation'] = const.NDMP_DATA_OP_RECOVER
        
        # Verify we have all variables for recover operation
        try:
            record.data['nlist'] = {
                        'original_path': bytes.decode(record.b.nlist[0].original_path).strip(),
                        'destination_dir': bytes.decode(record.b.nlist[0].destination_dir).strip(),
                        'new_name': bytes.decode(record.b.nlist[0].new_name).strip(),
                        'other_name': bytes.decode(record.b.nlist[0].other_name).strip(),
                        'node': ut.quad_to_long_long(record.b.nlist[0].node),
                        'fh_info': ut.quad_to_long_long(record.b.nlist[0].fh_info)
                        }
        except IndexError:
            stdlog.error('Invalid informations sent by DMA')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        
        # Generate the command line
        command_line = record.bu.recover(record)
        stdlog.debug(command_line)
        
        # release the Data Consumer
        self.record.data['sock'].file = open(record.bu['fifo'],'wb')
        stdlog.info('Starting recover of ' + self.record.data['env']['FILESYSTEM'])
        record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
        
        # Launch the recover process
        with open(record.bu['fifo'] + '.err', 'w', encoding='utf-8') as error:
            record.data['process'] = subprocess.Popen(shlex.split(command_line),
                                                   stdin=subprocess.PIPE,
                                                   stderr=error,
                                                   cwd=record.data['nlist']['destination_dir'],
                                                   shell=False)
    
    def reply_v4(self, record):
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
    
    def request_v4(self, record):
        pass
    
    def reply_v4(self, record):
        record.error = const.NDMP_NOT_SUPPORTED_ERR
    
    request_v3 = request_v4
    reply_v3 = reply_v4
    

class get_state():
    '''This request is used by the DMA to obtain information about the Data
       Server's operational state as represented by the Data Server variable
       set.'''
    
    def reply_v4(self, record):
        record.b.state = record.data['state']
        record.b.unsupported = (const.NDMP_DATA_STATE_EST_TIME_REMAIN_INVALID |
                                const.NDMP_DATA_STATE_EST_BYTES_REMAIN_INVALID)
        record.b.operation = record.data['operation']
        record.b.halt_reason = record.data['halt_reason']
        record.b.bytes_processed = ut.long_long_to_quad(0)
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
            
        stdlog.info('Bytes processed: ' + repr(0))

    reply_v3 = reply_v4

class get_env():
    '''This request is used by the DMA to obtain the backup environment
       variable set associated with the current data operation. The
       NDMP_DATA_GET_ENV request is typically issued following a successful
       backup operation but MAY be issued during or after a recovery
       operation as well.'''
    
    def reply_v4(self, record):
        record.b.env = []
        for var in record.data['bu'].env:
            record.b.env.append(type.ndmp_pval(name=repr(var).encode(), 
                                               value=repr(record.data['bu'].env[var]).encode()))

    reply_v3 = reply_v4

class stop():
    '''This request is used by the DMA to instruct the Data Server to
       release all resources, reset all Data Server state variables, reset
       all backup environment variables and transition the Data Server to
       the IDLE state.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_HALTED)
    def reply_v4(self, record):
        record.data['halt_reason'] = const.NDMP_DATA_HALT_NA
        record.data['state'] = const.NDMP_DATA_STATE_IDLE
        record.data['operation'] = const.NDMP_DATA_OP_NOACTION
                
    reply_v3 = reply_v4

class abort():
    '''This request is used by the DMA to instruct the Data Server to
       terminate any in progress data operation, close the data connection
       if present, and transition the Data Server to the HALTED state.'''
    
    @ut.valid_state(const.NDMP_DATA_STATE_IDLE, False)
    def reply_v4(self, record):
        try:
            record.data['bu'].handle_close()
        except AttributeError: # No BU defined
            pass
        record.data['server'].abort()
        record.data['halt_reason'] = const.NDMP_DATA_HALT_ABORTED
        record.data['state'] = const.NDMP_DATA_STATE_HALTED
        record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            
    reply_v3 = reply_v4