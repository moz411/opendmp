''' Mover Interface 
This interface controls the reading and writing of backup data 
from and to a tape device. During a backup the MOVER reads data 
from the data connection, buffers the data into tape records, and 
writes the data to the tape device. During a recover the Mover 
Interface reads data from the tape device and writes the data to 
the data connection. The MOVER handles tape exceptions and 
notifies the DMA. 
'''

import traceback, queue
from io import BufferedReader
from tools import utils as ut, ipaddress as ip
from server.mover import Mover
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from xdr.ndmp_type import (ndmp_tcp_addr_v4, ndmp_ipc_addr, 
                           ndmp_addr_v4, ndmp_tcp_addr, ndmp_addr_v3)


class set_record_size():
    '''This request is used by the DMA to establish the record size used for
       mover-initiated tape read and write operations.'''
    
    def request_v4(self, record):
        if(record.mover['state'] not in [const.NDMP_MOVER_STATE_IDLE]):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.mover['record_size'] = record.b.len
            record.mover['window_length'] = 0
            record.mover['window_offset'] = 0

    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4

class set_window():
    '''This request establishes a mover window in terms of offset and
       length. A mover window represents the portion of the overall backup
       stream that is accessible to the mover without intervening DMA tape
       manipulation.'''
    
    def request_v4(self, record):
        if(record.mover['state'] not in [const.NDMP_MOVER_STATE_IDLE,
                                     const.NDMP_MOVER_STATE_PAUSED]):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['record_size'] == 0):
            record.error = const.NDMP_PRECONDITION_ERR
        else:
            record.mover['window_offset'] = ut.quad_to_long_long(record.b.offset)
            record.mover['window_len'] = ut.quad_to_long_long(record.b.length)

    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4

class connect():
    '''This request is used by the DMA to instruct the mover to establish a
       data connection to a Data Server or peer mover.'''
    
    def request_v4(self, record):
        if(record.mover['state'] not in [const.NDMP_MOVER_STATE_IDLE]):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['record_size'] == 0):
            record.error = const.NDMP_PRECONDITION_ERR
        else:
            record.mover['state'] = const.NDMP_MOVER_STATE_ACTIVE
            record.mover['mode'] = record.b.mode
            record.mover['addr'] = record.b.addr
            
    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4

class listen():
    '''This request is used by the DMA to instruct the mover create a
       connection end point and listen for a subsequent data connection from
       a Data Server or peer Tape Server (mover). This request is also used
       by the DMA to obtain the address of connection end point the mover is
       listening at.'''
    
    def request_v4(self, record):
        if(record.mover['state'] != const.NDMP_MOVER_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.b.addr_type == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_IPC):
            pass
        elif(record.b.addr_type == const.NDMP_ADDR_TCP):
            record.mover['addr_type'] = const.NDMP_ADDR_TCP
            record.mover['mode'] = record.b.mode
            record.mover['qinfos'] = queue.Queue()
            t = Mover(record)
            t.start()
            record.mover['qinfos'].join()
            record.mover['host'] = record.mover['qinfos'].get()
            record.mover['port'] = record.mover['qinfos'].get()
            
            
    def reply_v4(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state != const.NDMP_MOVER_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['addr_type'] == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.mover['addr_type'] == const.NDMP_ADDR_IPC):
            pass
        elif(record.mover['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.connect_addr = ndmp_addr_v4
            record.b.connect_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.connect_addr.tcp_addr = []
            tcp_addr = ndmp_tcp_addr_v4(record.mover['host'],record.mover['port'],[])
            record.b.connect_addr.tcp_addr.append(tcp_addr)
            record.mover['peer'] = tcp_addr
            
    def reply_v3(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state != const.NDMP_MOVER_STATE_LISTEN):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['addr_type'] == const.NDMP_ADDR_LOCAL):
            pass
        elif(record.mover['addr_type'] == const.NDMP_ADDR_IPC):
            pass
        elif(record.mover['addr_type'] == const.NDMP_ADDR_TCP):
            host = record.mover['qinfos'].get()
            port = record.mover['qinfos'].get()
            addr = ip.IPv4Address(host)
            record.b.data_connection_addr = ndmp_addr_v3
            record.b.data_connection_addr.addr_type = const.NDMP_ADDR_TCP
            record.b.data_connection_addr.tcp_addr = ndmp_tcp_addr
            record.b.data_connection_addr.tcp_addr.ip_addr = addr._ip_int_from_string(host)
            record.b.data_connection_addr.tcp_addr.port = port

    request_v3 = request_v4

class read():
    '''This request is used by the DMA to instruct the mover to begin
       transferring the specified backup stream segment from the tape
       subsystem to the data connection.'''
    
    def request_v4(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state not in [const.NDMP_MOVER_STATE_ACTIVE]):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['window_length'] == 0):
            record.error = const.NDMP_PRECONDITION_ERR
        else:
            try:
                record.mover['buf'] = BufferedReader(record.mover['fd'], record.mover['window_length'])
                record.mover['buf'].read(record.mover['window_length'])
                record.mover['bytes_moved'] += record.mover['window_length']
                record.mover['bytes_left_to_read'] -= record.mover['window_length']
            except:
                record.error = const.NDMP_UNDEFINED_ERR
                stdlog.error('Mover read failed')
                stdlog.debug(traceback.print_exc())
            
    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4

class get_state():
    '''This request is used by the DMA to obtain information about the
       Mover's operational state as represented by the standard mover
       variable set.'''
    
    def reply_v4(self, record):
        with record.mover['lock']:
            record.b.state = record.mover['state']
            bytes_moved = record.mover['bytes_moved']
        record.b.mode =  record.mover['mode']
        record.b.pause_reason =  record.mover['pause_reason']
        record.b.halt_reason =  record.mover['halt_reason']
        record.b.record_size =  record.mover['record_size']
        record.b.record_num =  record.mover['record_num']
        record.b.bytes_moved =  ut.long_long_to_quad(bytes_moved)
        record.b.seek_position =  ut.long_long_to_quad(record.mover['seek_position'])
        record.b.bytes_left_to_read =  ut.long_long_to_quad(record.mover['bytes_left_to_read'])
        record.b.window_offset =  ut.long_long_to_quad(record.mover['window_offset'])
        record.b.window_length =  ut.long_long_to_quad(record.mover['window_length'])
        if(record.mover['addr_type'] == const.NDMP_ADDR_TCP):
            record.b.data_connection_addr = ndmp_addr_v4(const.NDMP_ADDR_TCP,[record.mover['peer']])
        elif(record.mover['addr_type'] == const.NDMP_ADDR_IPC):
            record.b.data_connection_addr = ndmp_ipc_addr(b'')
        
        stdlog.info('MOVER> Bytes moved: ' + repr(bytes_moved))
        #stdlog.info('MOVER> Bytes left to read: ' + repr(record.mover['bytes_left_to_read']))

    reply_v3 = reply_v4

class spec_continue():
    '''This request is used by the DMA to instruct the mover to transition
       from the PAUSED state to the ACTIVE state and to resume the transfer
       of data stream between the data connection and the tape subsystem.'''
    
    def reply_v4(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state != const.NDMP_MOVER_STATE_PAUSED):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        elif(record.mover['window_length'] == 0):
            record.error = const.NDMP_PRECONDITION_ERR
        else:
            record.mover['econt'].set()

    reply_v3 = reply_v4

class close():
    '''This request is used by the DMA to instruct the mover to gracefully
       close the current data connection and transition to the HALTED state.'''
    
    def reply_v4(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state != const.NDMP_MOVER_STATE_PAUSED):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.mover['equit'].set() # Will close the mover thread
            with record.mover['lock']:
                record.mover['state'] = const.NDMP_MOVER_STATE_HALTED

    reply_v3 = reply_v4

class stop():
    '''This request is used by the DMA to instruct the mover to release all
       resources, reset all mover state variables (except record_size), and
       transition the mover to the IDLE state.'''
    
    def reply_v4(self, record):
        record.mover['equit'].set() # Will close the mover thread
        with record.mover['lock']:
            state = record.mover['state']
        if(state == const.NDMP_MOVER_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
                record.mover['mode'] = const.NDMP_MOVER_MODE_NOACTION
                with record.mover['lock']:
                    record.mover['state'] = const.NDMP_MOVER_STATE_IDLE
                record.mover['fd'] = None
                record.mover['halt_reason'] = const.NDMP_MOVER_HALT_NA
                record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_NA
                record.mover['record_num'] = 0
                record.mover['bytes_moved'] = 0 
                record.mover['seek_position'] = 0
                record.mover['bytes_left_to_read'] = 0
                record.mover['window_length'] = 0 
                record.mover['window_offset'] = 0
                
    reply_v3 = reply_v4
    
class abort():
    '''This request is used by the DMA to instruct the mover to terminate
       any in progress mover operation, close the data connection if
       present, and transition the mover to the to the HALTED state.'''
    
    def reply_v4(self, record):
        with record.mover['lock']:
            state = record.mover['state']
        if(state == const.NDMP_MOVER_STATE_IDLE):
            record.error = const.NDMP_ILLEGAL_STATE_ERR
        else:
            record.mover['equit'].set() # Will close the mover thread
            record.mover['mode'] = const.NDMP_MOVER_MODE_NOACTION
            with record.mover['lock']:
                record.mover['state'] = const.NDMP_MOVER_STATE_HALTED
            record.mover['fd'] = None
            record.mover['halt_reason'] = const.NDMP_MOVER_HALT_ABORTED
            record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_NA
            record.mover['record_num'] = 0
            record.mover['bytes_moved'] = 0 
            record.mover['seek_position'] = 0
            record.mover['bytes_left_to_read'] = 0
            record.mover['window_length'] = 0 
            record.mover['window_offset'] = 0

    reply_v3 = reply_v4