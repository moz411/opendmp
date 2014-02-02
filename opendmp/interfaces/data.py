''' Data Interface 
This interface initiates backup and recover operations. The DMA 
provides all the parameters that affect the backup or recovery 
using the Data Interface. The DMA does not place any constraints 
on the format of the backup data other than it MUST be a stream 
of data that can be written to the tape device. 
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config; threads = Config.threads
import os, subprocess, shlex, time, socket, asyncore
from server.data import Data
from server.fh import Fh
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut, ipaddress as ip
from interfaces import notify as nt

class connect(asyncore.dispatcher):
    '''This request is used by the DMA to instruct the Data Server to
        establish a data connection to a Tape Server or peer Data Server'''
    
    def request_v4(self, record):
        self.record = record
        if(self.record.data['state'] != const.NDMP_DATA_STATE_IDLE):
            self.record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.set_reuse_addr()
            self.record.data['addr_type'] = self.record.b.addr_type
            ip_int = self.record.b.tcp_addr[0].ip_addr
            host = ip.IPv4Address(ip_int)._string_from_ip_int(ip_int)
            self.connect((host, self.record.b.tcp_addr[0].port))
            self.record.data['state'] = const.NDMP_DATA_STATE_CONNECTED
            self.record.data['host'], self.record.data['port'] = self.socket.getpeername()
            self.record.data['peer'] = self.socket
            stdlog.info('DATA> Connected to ' + self.record.data['host'] + ': ' + repr(self.record.data['port']))
            
    def reply_v4(self, record):
        pass
    
    def handle_error(self):
        self.record.error = const.NDMP_DATA_HALT_CONNECT_ERROR
        stdlog.error('DATA> Connection failed')

    request_v3 = request_v4
    reply_v3 = reply_v4


class listen():
    '''This request is used by the DMA to instruct the Data Server create a
       connection end point and listen for a subsequent data connection from
       a Tape Server (mover) or peer Data Server. This request is also used
       by the DMA to obtain the address of connection end point the Data
       Server is listening at.'''
    
    def request_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        else:
            record.data['addr_type'] = record.b.addr_type
            fd = ip.get_next_data_conn()
            (hostname, port) = fd.getsockname()
            fd.close()
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.set_reuse_addr()
            self.bind((hostname, port))
            self.listen(1)
            host, record.data['port'] = self.getsockname()
            record.data['host'] = ip.ip_address(host)._ip_int_from_string(host)
            record.data['state'] = const.NDMP_DATA_STATE_LISTEN
            self.record = record
        
        
    def reply_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.b.connect_addr = type.ndmp_addr_v4()
            record.b.connect_addr.addr_type = record.data['addr_type']
        if(record.mover['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.connect_addr.tcp_addr = []
            tcp_addr = type.ndmp_tcp_addr_v4(record.data['host'],record.data['port'],[])
            record.b.connect_addr.tcp_addr.append(tcp_addr)
            record.data['peer'] = tcp_addr
                
    def reply_v3(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.b.connect_addr = type.ndmp_addr_v4()
            record.b.connect_addr.addr_type = record.data['addr_type']
            if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
                record.b.data_connection_addr = type.ndmp_addr_v3
                record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
                record.b.data_connection_addr.tcp_addr = type.ndmp_tcp_addr
                record.b.data_connection_addr.tcp_addr.ip_addr = record.data['host']
                record.b.data_connection_addr.tcp_addr.port = record.data['port']
            
    request_v3 = request_v4
    
    def handle_accepted(self, connection, address):
        stdlog.info('DATA> Connection from ' + repr(address))
        # Start an asyncore Consumer for this connection
        Data(connection, self.record)
        self.record.data['state'] = const.NDMP_DATA_STATE_ACTIVE

class start_backup():
    '''This request is used by the DMA to instruct the Data Server to
       initiate a backup operation and begin transferring backup data from
       the file system represented by this Data Server to a Tape Server or
       peer Data Server over the previously established data connection.'''
    
    def request_v4(self, record):
        bu_type = bytes.decode(record.b.bu_type).strip()
        record.data['operation'] = const.NDMP_DATA_OP_BACKUP
        
        if(record.data['state'] != const.NDMP_DATA_STATE_CONNECTED):
            stdlog.error('Illegal state for start_backup: ' + const.ndmp_data_state[record.data['state']])
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        # TODO: implement a real plugin system
        elif not((c.system in c.Unix and bu_type in ['tar', 'dump']) or
           (c.system in c.Windows and bu_type in ['wbadmin', 'ntbackup'])):
            stdlog.error('BUTYPE ' + bu_type + ' not supported')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        else:
            # TODO: implement a real plugin system
            record.data['bu_type'] = bu_type
            from bu import tar
            Bu = tar.Bu()

        # Extract all env variables, overwrite default_env
        for pval in tar.info.default_env:
            name = pval.name.decode().strip()
            value = pval.value.decode().strip()
            record.data['env'][name] =  value
        for pval in record.b.env:
            name = pval.name.decode().strip()
            value = pval.value.decode('utf-8', 'replace').strip()
            record.data['env'][name] =  value
        
        # Retrieving FILESYSTEM to backup
        try:
            if(record.data['env']['FILES']):
                record.data['env']['FILESYSTEM'] = record.data['env']['FILES']
        except KeyError:
            pass
        try:
            assert(record.data['env']['FILESYSTEM'] != None)
        except (KeyError, AssertionError):
            stdlog.error('variable FILESYSTEM does not exists')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        try:
            assert(os.path.exists(record.data['env']['FILESYSTEM']))
        except (KeyError, AssertionError):
            stdlog.error('FILESYSTEM ' + record.data['env']['FILESYSTEM'] + ' does not exists')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        
        # Generate the command line
        command_line = Bu.backup(record)
        stdlog.debug(command_line)
        
        # Launch the backup process
        with open(record.fh['history'], 'w', encoding='utf-8') as listing:
            with open(record.data['bu_fifo'] + '.err', 'w', encoding='utf-8') as error:
                record.data['process'] = subprocess.Popen(shlex.split(command_line), 
                                                          cwd=record.data['env']['FILESYSTEM'],
                                                          stdout=listing, stderr=error, shell=False)
        
        
        # Launch the backup asyncore Consumer
        Data(record)
        record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
        
        # Launch the File History asyncore Consumer
        Fh(record)
        
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
    
    def request_v4(self, record):
        bu_type = bytes.decode(record.b.bu_type).strip()
        record.data['operation'] = const.NDMP_DATA_OP_RECOVER
        
        if(record.data['state'] != const.NDMP_DATA_STATE_CONNECTED):
            stdlog.error('Illegal state for start_backup: ' + const.ndmp_data_state[record.data['state']])
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        # TODO: implement a real plugin system
        elif not((c.system in c.Unix and bu_type in ['tar', 'dump']) or
           (c.system in c.Windows and bu_type in ['wbadmin', 'ntbackup'])):
            stdlog.error('BUTYPE ' + bu_type + ' not supported')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        else:
            # TODO: implement a real plugin system
            record.data['bu_type']  = bu_type
            from bu import tar
            Bu = tar.Bu()

        # Extract all env variables, overwrite default_env
        for pval in tar.info.default_env:
            name = pval.name.decode().strip()
            value = pval.value.decode().strip()
            record.data['env'][name] =  value
        for pval in record.b.env:
            name = pval.name.decode().strip()
            value = pval.value.decode('utf-8', 'replace').strip()
            record.data['env'][name] =  value
            
        # Retrieving FILESYSTEM to recover
        try:
            if(record.data['env']['FILES']):
                record.data['env']['FILESYSTEM'] = record.data['env']['FILES']
        except KeyError:
            pass
        try:
            assert(record.data['env']['FILESYSTEM'] != None)
        except (KeyError, AssertionError) as e:
            stdlog.error('variable FILESYSTEM does not exist')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        try:
            assert(os.path.exists(record.data['nlist']['destination_dir']))
        except (KeyError, AssertionError) as e:
            stdlog.error('destination directory does not exist')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
            
            
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
        try:
            command_line = Bu.recover(record)
        except (OSError, AttributeError, KeyError) as e:
            stdlog.error(e)
            record.error = const.NDMP_NOT_SUPPORTED_ERR
            return
        stdlog.debug(command_line)
        
        # Launch the backup asyncore Consumer
        Data(record)
        record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
        
        # Launch the recover process
        with open(record.data['bu_fifo'] + '.err', 'w', encoding='utf-8') as error:
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
        sent = record.data['stats']['current']
        record.b.state = record.data['state']
        remain = record.data['stats']['total'] - sent
        record.b.unsupported = (const.NDMP_DATA_STATE_EST_TIME_REMAIN_INVALID |
                                const.NDMP_DATA_STATE_EST_BYTES_REMAIN_INVALID)
        record.b.operation = record.data['operation']
        record.b.halt_reason = record.data['halt_reason']
        record.b.bytes_processed = ut.long_long_to_quad(sent)
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
            
        stdlog.info('DATA> Bytes processed: ' + repr(sent))
        #stdlog.info('DATA> Bytes remaining: ' + repr(remain))

    reply_v3 = reply_v4

class get_env():
    '''This request is used by the DMA to obtain the backup environment
       variable set associated with the current data operation. The
       NDMP_DATA_GET_ENV request is typically issued following a successful
       backup operation but MAY be issued during or after a recovery
       operation as well.'''
    
    def reply_v4(self, record):
        record.b.env = []
        for var in record.data['env']:
            record.b.env.append(type.ndmp_pval(name=repr(var).encode(), 
                                               value=repr(record.data['env'][var]).encode()))

    reply_v3 = reply_v4

class stop():
    '''This request is used by the DMA to instruct the Data Server to
       release all resources, reset all Data Server state variables, reset
       all backup environment variables and transition the Data Server to
       the IDLE state.'''
    
    def reply_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_HALTED):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.data['halt_reason'] = const.NDMP_DATA_HALT_NA
            record.data['state'] = const.NDMP_DATA_STATE_IDLE
            record.data['operation'] = const.NDMP_DATA_OP_NOACTION
                
    reply_v3 = reply_v4

class abort():
    '''This request is used by the DMA to instruct the Data Server to
       terminate any in progress data operation, close the data connection
       if present, and transition the Data Server to the HALTED state.'''
    
    def reply_v4(self, record):
        if(record.data['state'] == const.NDMP_DATA_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else: 
            try:
                record.data['process'].kill()
                record.data['process'].wait()
            except OSError as e:
                stdlog.error('Cannot stop process ' + repr(record.data['process'].pid) + ':' + e.strerror)
            except AttributeError:
                stdlog.error('Process already stopped')
            
            record.data['halt_reason'] = const.NDMP_DATA_HALT_ABORTED
            record.data['state'] = const.NDMP_DATA_STATE_HALTED
            record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            
    reply_v3 = reply_v4