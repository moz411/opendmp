import struct, time, random, hashlib, traceback, queue
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const, ndmp_type as type
from tools import utils as ut
from xdrlib import Error as XDRError
from xdr.ndmp_pack import NDMPPacker, NDMPUnpacker

class Record():
    '''This class will traverse the whole NDMP session, and keep
    all the needed informations (states, sockets, sequences, etc)
    that is shared in the program'''
    
    def __init__(self):
        self.queue = queue.Queue()
        self.challenge = random.randint(0, 2**64).to_bytes(64, 'big')
        self.h = type.ndmp_header()
        self.b = None
        self.error = const.NDMP_NO_ERR
        self.server_sequence = 2
        self.dma_sequence = 0
        self.protocol_version = cfg['PREFERRED_NDMP_VERSION']
        self.connected = False
        self.device = None
        self.p = NDMPPacker()
        self.u = NDMPUnpacker(b'None')
        
        self.log = {'type': const.NDMP_LOG_NORMAL,
               'id': 0,
               'entry': None
               }
    
        self.data = {'type': None, 
                'env': {}, 
                'state': const.NDMP_DATA_STATE_IDLE,
                'operation': const.NDMP_DATA_OP_NOACTION,
                'addr_type': const.NDMP_ADDR_LOCAL,
                'halt_reason': const.NDMP_DATA_HALT_NA,
                'fd': None,
                'local_address': None,
                'host': None,
                'port': None,
                'bu_fifo': None,
                'stack_size': 10240,
                'log': None,
                'error': [],
                'process': None,
                'retcode': 255,
                'filesystem': None,
                'offset': 0,
                'length': 2**64-1,
                'file': None,
                'recovery': {'status': None},
                'dumpdates': {},
                'stampfile': None,
                'stats':  {'total': 0,
                           'current': [0]}}
        
        self.mover = {'mode': const.NDMP_MOVER_MODE_NOACTION,
                 'state': const.NDMP_MOVER_STATE_IDLE,
                 'addr_type': None,
                 'host': None,
                 'port': None,
                 'peer': None,
                 'halt_reason': const.NDMP_MOVER_HALT_NA,
                 'pause_reason': const.NDMP_MOVER_PAUSE_NA,
                 'record_size': 0,
                 'record_num': 0,
                 'bytes_moved': [0],
                 'seek_position': 0,
                 'bytes_left_to_read': 0,
                 'window_length': 0, 
                 'window_offset': 0}
        
        self.fh = {'files': [],
                  'history': None,
                  'max_lines': 1000}
        
    def __repr__(self):
        out = []
        if(self.h.message_type == const.NDMP_MESSAGE_REPLY):
            out += ['seq=%d' % self.h.sequence]
            out += ['reply_seq=%d' % self.h.reply_sequence]
            out += ['%s' % const.ndmp_message[self.h.message]]
            out += ['%s' % const.ndmp_error[self.error]]
            out += ['conn=%s' % self.connected]
            if (self.h.message in [ const.NDMP_DATA_ABORT, const.NDMP_DATA_CONNECT,
                                   const.NDMP_DATA_CONTINUE, const.NDMP_DATA_GET_ENV,
                                   const.NDMP_DATA_GET_STATE ] ):
                out += ['ds=%s' % const.ndmp_data_state[self.data['state']]]
                out += ['do=%s' % const.ndmp_data_operation[self.data['operation']]]
            if (self.h.message in [ const.NDMP_MOVER_ABORT, const.NDMP_MOVER_CLOSE,
                                   const.NDMP_MOVER_CONNECT, const.NDMP_MOVER_CONTINUE,
                                   const.NDMP_MOVER_GET_STATE, const.NDMP_MOVER_LISTEN]):
                out += ['%s' % const.ndmp_mover_state[self.mover['state']]]
                out += ['%s' % const.ndmp_mover_mode[self.mover['mode']]]
            if (self.h.message in [ const.NDMP_TAPE_CLOSE, const.NDMP_TAPE_EXECUTE_CDB,
                                   const.NDMP_TAPE_GET_STATE, const.NDMP_TAPE_MTIO,
                                   const.NDMP_TAPE_OPEN, const.NDMP_TAPE_READ, 
                                   const.NDMP_TAPE_SET_RECORD_SIZE, const.NDMP_TAPE_UNLOAD,
                                   const.NDMP_TAPE_WRITE, const.NDMP_SCSI_CLOSE,
                                   const.NDMP_SCSI_EXECUTE_CDB, const.NDMP_SCSI_GET_STATE,
                                   const.NDMP_SCSI_OPEN, const.NDMP_SCSI_RESET_BUS,
                                   const.NDMP_SCSI_RESET_DEVICE, const.NDMP_SCSI_SET_TARGET]):
                out += ['%s' % repr(self.device)]
        else:
            out += ['seq=%d' % self.h.sequence]
            out += ['reply_seq=%d' % self.h.reply_sequence]
            out += ['%s' % const.ndmp_message[self.h.message]]
        return '%s' % ', '.join(out)
    __str__ = __repr__   
    
    def run_task(self, message):
        
        # First part: decode and execute the request
        # Unpack header
        self.u.reset(message)
        self.h = self.u.unpack_ndmp_header()
        self.error = self.h.error
        
        if self.h.message in [const.NDMP_CONNECT_CLOSE, const.NDMP_SHUTDOWN]:
                    return
                
        # debug
        stdlog.debug(repr(self))
        
        # Unpack body
        if(self.verify_sequence()):
            self.body()
            
        # Second part: prepare and send the response    
        self.reset()
        self.h.message_type = const.NDMP_MESSAGE_REPLY
        self.h.reply_sequence = self.dma_sequence
        self.h.sequence = self.server_sequence
        self.server_sequence+=1
        self.h.time_stamp = int(time.time())
        
        # Pack header
        self.h.error = self.error
        self.p.pack_ndmp_header(self.h)
        
        # Prepare and pack body
        if(self.verify_connected() and self.error == const.NDMP_NO_ERR):
            self.body()
            
        # debug
        stdlog.debug(repr(self))
        stdlog.debug('\t' + repr(self.b))
        stdlog.debug('')
        
        # add the encoded answer to the message queue
        self.queue.put(self.p.get_buffer())
        
        
    def body(self):
        """
        Depending of the NDMP message
        received, will load the corresponding src/interfaces/<interface>.py
        and run the corresponding function.
        Example: NDMP_SCSI_SET_TARGET <-> function set_target in src/interfaces/scsi.py
        """
        message = const.ndmp_message[self.h.message].lower()
        # Exceptions for names that conflict with reserved keywords
        interface = 'interfaces.' + str(message.split('_',2)[1])
        func = str(message.split('_',2)[2])
        if(self.h.message in [const.NDMP_MOVER_CONTINUE]):
            func = ('spec_' + func)

        try: 
            exec('from ' + interface + ' import ' + func)
        except:
            self.error =  const.NDMP_NOT_SUPPORTED_ERR
            stdlog.error(message + '_reply not supported')
            stdlog.debug(traceback.print_exc())
            return

        if (self.h.message_type == const.NDMP_MESSAGE_REPLY):
            try:
                exec('self.b = type.' + message + '_reply_' + self.protocol_version + '()')
            except NameError:
                self.error =  const.NDMP_NOT_SUPPORTED_ERR
                stdlog.error(message + '_reply not supported')
                stdlog.debug(traceback.print_exc())
                return
            
            try:
                #run "reply" function
                exec(func + '().reply_' + self.protocol_version + '(self)')
            except:
                self.error =  const.NDMP_NOT_SUPPORTED_ERR
                stdlog.error(message + '_reply not supported')
                stdlog.debug(traceback.print_exc())
                return
            
            try:
                self.b.error = self.error
                exec('self.p.pack_' + message + '_reply_' + self.protocol_version +'(self.b)')
            except (TypeError, XDRError, AttributeError, NameError, struct.error):
                self.error =  const.NDMP_XDR_ENCODE_ERR
                stdlog.error('Error processing message ' + message+ '_reply_' + self.protocol_version)
                stdlog.debug(traceback.print_exc())
                
        elif (self.h.message_type == const.NDMP_MESSAGE_REQUEST and 
              # This request does not have a message body
              self.h.message not in [const.NDMP_CONFIG_GET_HOST_INFO, const.NDMP_CONFIG_GET_SERVER_INFO, 
                                     const.NDMP_CONFIG_GET_CONNECTION_TYPE, const.NDMP_CONFIG_GET_BUTYPE_INFO,
                                     const.NDMP_CONFIG_GET_FS_INFO, const.NDMP_CONFIG_GET_TAPE_INFO,
                                     const.NDMP_CONFIG_GET_SCSI_INFO, const.NDMP_CONFIG_GET_EXT_LIST,
                                     const.NDMP_SCSI_CLOSE, const.NDMP_SCSI_GET_STATE,
                                     const.NDMP_SCSI_RESET_DEVICE, const.NDMP_TAPE_CLOSE,
                                     const.NDMP_TAPE_GET_STATE, const.NDMP_DATA_GET_STATE,
                                     const.NDMP_DATA_GET_ENV, const.NDMP_DATA_STOP,
                                     const.NDMP_DATA_ABORT, const.NDMP_MOVER_GET_STATE,
                                     const.NDMP_MOVER_CONTINUE, const.NDMP_MOVER_CLOSE,
                                     const.NDMP_MOVER_ABORT, const.NDMP_MOVER_STOP, const.NDMP_CONNECT_CLOSE]):
            try:
                #prepare unpack function
                exec('self.b  = self.u.unpack_' + message + '_request_' + self.protocol_version +'()')
                # debug
                stdlog.debug('\t' + repr(self.b))
                stdlog.debug('')
                # run "request" function
                exec(func + '().request_' + self.protocol_version +'(self)')
            except (TypeError, XDRError, AttributeError, EOFError):
                self.error =  const.NDMP_XDR_DECODE_ERR
                stdlog.error('Error processing message ' + message + '_request_' + self.protocol_version)
                stdlog.debug(traceback.print_exc())
                return
            except:
                self.error =  const.NDMP_NOT_SUPPORTED_ERR
                stdlog.error(message + '_request not supported')
                stdlog.debug(traceback.print_exc())
                return    
    
    def verify_connected(self):
        if(self.connected):
            return True
        elif (self.h.message in [const.NDMP_CONNECT_OPEN, const.NDMP_CONNECT_CLOSE,
                                 const.NDMP_CONFIG_GET_SERVER_INFO, const.NDMP_CONFIG_GET_AUTH_ATTR]):
            return True
        elif(self.h.message == const.NDMP_CONNECT_CLIENT_AUTH):
            try:
                assert(self.auth.auth_type == const.NDMP_AUTH_MD5)
                assert(self.auth.auth_data.auth_md5.auth_digest == self.auth_md5())
                self.connected = True
                return True
            except (AttributeError, AssertionError):
                self.error = const.NDMP_NOT_AUTHORIZED_ERR
                return False
        else:
            self.error = const.NDMP_NOT_AUTHORIZED_ERR
            return False

    def verify_sequence(self):
        if(self.h.message_type == const.NDMP_MESSAGE_REPLY
           and self.h.reply_sequence != self.server_sequence):
            self.error =  const.NDMP_SEQUENCE_NUM_ERR
        elif (self.h.message_type == const.NDMP_MESSAGE_REQUEST
              and self.h.message != const.NDMP_CONNECT_OPEN
              and self.h.sequence != self.dma_sequence+1):
            self.error =  const.NDMP_SEQUENCE_NUM_ERR
        else:
            self.dma_sequence = self.h.sequence
            return True
    
    def auth_md5(self):
        password = cfg['PASSWORD'].encode()
        if(self.auth.auth_data.auth_md5.auth_id != cfg['USER'].encode()):
            self.error =  const.NDMP_NOT_AUTHORIZED_ERR
        #TODO: Still something to fix for passwd > 32 bytes
        m = hashlib.md5()
        if(len(password) == 0):
            self.error =  const.NDMP_NOT_AUTHORIZED_ERR
        elif(len(password) > 32): 
            padding = b''
            password = bytes((password)[:32])
        else: 
            padding = b'\x00'*(64-2*len(password))
        m.update(password + padding + self.challenge + password)
        return(m.digest())

    def reset(self):
        self.b = ut.Empty()
        self.message = b''
        self.p.reset()
        
    def close(self):
        try:
            self.device.close(self)
        except (AttributeError, KeyError):
            pass
        self.device = None
        try:
            from interfaces import mover, data
            mover.stop().reply_v4(self)
            data.stop().reply_v4(self)
        except (AttributeError, KeyError):
            pass