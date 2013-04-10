''' Data Interface 
This interface initiates backup and recover operations. The DMA 
provides all the parameters that affect the backup or recovery 
using the Data Interface. The DMA does not place any constraints 
on the format of the backup data other than it MUST be a stream 
of data that can be written to the tape device. 
'''
import traceback, os, shlex
from subprocess import Popen, PIPE
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from server.data import Data
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut, ipaddress as ip, butypes as bu, betterwalk as bt

class connect():
    '''This request is used by the DMA to instruct the Data Server to
        establish a data connection to a Tape Server or peer Data Server'''
    
    def request_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        elif(record.b.addr_type == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_IPC):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_TCP):
            record.data['addr_type'] = const.NDMP_ADDR_TCP
            # Try to use the fastest connection in the given list
            try:
                #record.data['fd'] = ip.get_best_data_conn(record.b.tcp_addr)
                record.data['fd'] = ip.get_data_conn(record.b.tcp_addr)
                record.data['state'] = const.NDMP_DATA_STATE_CONNECTED
            except:
                record.error = const.NDMP_DATA_HALT_CONNECT_ERROR
                stdlog.error('DATA> Cannot connect to ' + repr(record.data['peer']))
                stdlog.debug(traceback.print_exc())
            record.data['peer'] = record.data['fd'].getsockname()
            
            
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
    
    def request_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        elif(record.b.addr_type == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_IPC):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_TCP):
            record.data['fd'] = ip.get_next_data_conn()
            record.data['addr_type'] = const.NDMP_ADDR_TCP
            record.data['state'] = const.NDMP_DATA_STATE_LISTEN
        
    def reply_v4(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        elif(record.data['addr_type'] == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.data['addr_type'] == const.NDMP_ADDR_IPC):
            pass
        elif(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            (host, port) = record.data['fd'].getsockname()
            addr = ip.IPv4Address(host)
            record.b.data_connection_addr = type.ndmp_addr_v4()
            record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.data_connection_addr.tcp_addr = []
            tcp_addr = type.ndmp_tcp_addr_v4(addr._ip_int_from_string(host),port,[])
            record.b.data_connection_addr.tcp_addr.append(tcp_addr)
            
    def reply_v3(self, record):
        if(record.data['state'] != const.NDMP_DATA_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
            return
        elif(record.data['addr_type'] == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.data['addr_type'] == const.NDMP_ADDR_IPC):
            pass
        elif(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            (host, port) = record.data['fd'].getsockname()
            addr = ip.IPv4Address(host)
            record.b.data_connection_addr = type.ndmp_addr_v3()
            record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.data_connection_addr.tcp_addr = type.ndmp_tcp_addr()
            record.b.data_connection_addr.tcp_addr.ip_addr = addr._ip_int_from_string(host)
            record.b.data_connection_addr.tcp_addr.port = port
            
    request_v3 = request_v4

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
        elif not((c.system in c.Unix and bu_type in bu.Unix) or
           (c.system in c.Windows and bu_type in bu.Windows)):
            stdlog.error('BUTYPE ' + bu_type + ' not supported')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        else:
            exec('record.data[\'type\']  = bu.' + bu_type)
            #exec('from tools import ' + bu_type)

        # Extract all env variables, overwrite default_env
        for pval in record.data['type'].default_env:
            name = bytes.decode(pval.name).strip()
            value = bytes.decode(pval.value).strip()
            record.data['env'][name] =  value
        for pval in record.b.env:
            name = bytes.decode(pval.name).strip()
            value = bytes.decode(pval.value).strip()
            record.data['env'][name] =  value
        try:
            assert(record.data['env']['FILESYSTEM'] != None)
        except:
            try:
                record.data['env']['FILESYSTEM'] = record.data['env']['FILES']
            except:
                record.error = const.NDMP_ILLEGAL_ARGS_ERR
                return
        if not(os.path.exists(record.data['env']['FILESYSTEM'])):
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
            
        '''Retrieve the size of all files in the given path,
        taking care of symlinks and hardlinks.
        It also fill the record.data['stats']['files'] list'''
        stdlog.info('Getting size of %s' % record.data['env']['FILESYSTEM'])
        seen = {}
        for dirpath, dirnames, filenames in bt.walk(record.data['env']['FILESYSTEM']):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    stat = os.stat(fp)
                    record.data['stats']['files'].append([fp, stat])
                    record.data['stats']['total'] += stat.st_size
                except OSError:
                    continue
                
                try:
                    seen[stat.st_ino]
                except KeyError:
                    seen[stat.st_ino] = True
                else:
                    continue
        stdlog.info(ut.approximate_size(record.data['stats']['total']))

        try:
            # Launch the bu process, sending data directly to the socket
            record.data['process'] = Popen([bu_type, record.data['type'].options['backup'], 
                                            record.data['env']['FILESYSTEM']],
                                                   stdout=PIPE,
                                                   stderr=PIPE,
                                                   shell=False,
                                                   bufsize=10240)
            t = Data(record)
            t.start()
            #record.data['process'].wait()
            record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
            record.data['estart'].set()
        except (OSError, ValueError):
            record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            stdlog.error('Backup of %s failed' % record.data['env']['FILESYSTEM'])
            stdlog.debug(traceback.print_exc())
            stdlog.error(record.data['error'])

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
        elif not((c.system in c.Unix and bu_type in bu.Unix) or
           (c.system in c.Windows and bu_type in bu.Windows)):
            stdlog.error('BUTYPE ' + bu_type + ' not supported')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        else:
            exec('record.data[\'type\']  = bu.' + bu_type)

        # Extract all env variables, overwrite default_env
        for pval in record.data['type'].default_env:
            name = bytes.decode(pval.name).strip()
            value = bytes.decode(pval.value).strip()
            record.data['env'][name] =  value
        for pval in record.b.env:
            name = bytes.decode(pval.name).strip()
            value = bytes.decode(pval.value).strip()
            record.data['env'][name] =  value
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
        if not (os.path.exists(record.data['nlist']['destination_dir'])):
            stdlog.info('Path ' + record.data['nlist']['destination_dir'] +
                        ' does not exists, creating')
            try:
                os.makedirs(record.data['nlist']['destination_dir'])
            except OSError as e:
                stdlog.error(e.strerror)
                record.error = const.NDMP_UNDEFINED_ERR
        
        try:
            # Launch the bu process, sending data directly to the socket
            command = ' '.join([bu_type, record.data['type'].options['recover'],
                               record.data['nlist']['destination_dir']])
            record.data['process'] = Popen(shlex.split(command),
                                                   stdin=PIPE,
                                                   stderr=PIPE,
                                                   shell=False,
                                                   bufsize=10240)
            t = Data(record)
            t.start()
            #record.data['process'].wait()
            record.data['state'] = const.NDMP_DATA_STATE_ACTIVE
        except (OSError, ValueError):
            record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            stdlog.error('Recover of %s failed' % record.data['nlist']['original_path'])
            stdlog.debug(traceback.print_exc())
            stdlog.error(record.data['error'])
    
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
        with record.data['lock']:
            sent = record.data['stats']['current']
            try:
                record.data['error'] = record.data['process'].stderr
            except AttributeError:
                record.data['error'] = const.NDMP_NO_ERR
            record.b.state = record.data['state']
            remain = record.data['stats']['total'] - sent
            record.b.unsupported = const.NDMP_DATA_STATE_EST_TIME_REMAIN_INVALID
            record.b.operation = record.data['operation']
            record.b.halt_reason = record.data['halt_reason']
            record.b.bytes_processed = ut.long_long_to_quad(sent)
            record.b.est_bytes_remain = ut.long_long_to_quad(remain)
            record.b.est_time_remain = 0
            record.b.invalid = 0
        if(record.data['addr_type'] == const.NDMP_ADDR_TCP):
            intaddr = ip.ip_address(record.data['peer'][0])
            addr = intaddr._ip_int_from_string(record.data['peer'][0])
            record.b.data_connection_addr = type.ndmp_addr_v4(const.NDMP_ADDR_TCP,
                                                         [type.ndmp_tcp_addr_v4(addr,
                                                                           record.data['peer'][1],
                                                             [type.ndmp_pval(name=b'', value=b'')])])
        elif(record.data['addr_type'] == const.NDMP_ADDR_IPC):
            record.b.data_connection_addr = type.ndmp_ipc_addr(b'')
        elif(record.data['addr_type'] == const.NDMP_ADDR_LOCAL):
            record.b.data_connection_addr = type.ndmp_addr_v4(const.NDMP_ADDR_LOCAL)
        record.b.read_offset = ut.long_long_to_quad(0)
        record.b.read_length = ut.long_long_to_quad(0)
            
        stdlog.info('DATA> Bytes processed: ' + repr(sent))
        stdlog.info('DATA> Bytes remaining: ' + repr(remain))

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
        record.data['equit'].set()
        with record.data['lock']:
            state = record.data['state']
        if(state != const.NDMP_DATA_STATE_HALTED):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            with record.data['lock']:
                record.data['state'] = const.NDMP_DATA_STATE_IDLE
            record.data['type'] = None
            record.data['env'] = {}
            record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            record.data['filesystem'] = None
            record.data['stats'] =  {'total': 0,'current': 0,'files': []}

    reply_v3 = reply_v4

class abort():
    '''This request is used by the DMA to instruct the Data Server to
       terminate any in progress data operation, close the data connection
       if present, and transition the Data Server to the HALTED state.'''
    
    def reply_v4(self, record):
        with record.data['lock']:
            state = record.data['state']
        if(state == const.NDMP_DATA_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            try:
                record.data['process'].poll
                if (record.data['process'].returncode == None):
                    record.data['process'].terminate()
            except OSError as e:
                stdlog.error('Cannot stop process ' + repr(record.data['process'].pid) + ':' + e.strerror)
            record.mover['equit'].set() # Will close the data thread
            record.data['halt_reason'] = const.NDMP_DATA_HALT_ABORTED
            record.data['state'] = const.NDMP_DATA_STATE_HALTED
            record.data['type'] = None
            record.data['env'] = {}
            record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            record.data['filesystem'] = None
            record.data['stats'] =  {'total': 0,'current': 0,'files': []}
                
    reply_v3 = reply_v4