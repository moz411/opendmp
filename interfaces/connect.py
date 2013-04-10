'''This interface authenticates the client and negotiates the version of
   protocol to be used.

   The DMA first connects to a well-known port (10,000). The NDMP Server
   accepts the connection and sends an NDMP_NOTIFY_CONNECTION_STATUS
   message. The DMA then sends an NDMP_CONNECT_OPEN message. The DMA is
   authenticated by the NDMP Server using an NDMP_CONNECT_CLIENT_AUTH
   message. Optionally, the DMA MAY use an NDMP_CONNECT_SERVER_AUTH
   message to authenticate the NDMP Server as well.

   If any of the Connect Interface messages fail, the DMA SHOULD close
   the connection using NDMP_CONNECTION_CLOSE.'''

import xdr.ndmp_const as const
from server.config import Config; cfg = Config.cfg
from server.log import Log; stdlog = Log.stdlog

class open():
    '''This message negotiates the protocol version to be used between the
        DMA and NDMP Server. '''
    
    def request_v4(self, record):
        record.protocol_version = 'v' + repr(record.b.protocol_version)
        

    def reply_v4(self, record):
        if(record.protocol_version in cfg['SUPPORTED_NDMP_VERSIONS']):
            record.error = const.NDMP_NO_ERR
        else:
            record.error =  const.NDMP_ILLEGAL_ARGS_ERR
    
    request_v3 = request_v4
    reply_v3 = reply_v4


class client_auth():
    '''This request authenticates the DMA to a NDMP Server.'''
    
    def request_v4(self, record):
        record.auth = record.b

    def reply_v4(self, record):
        record.auth = None

    request_v3 = request_v4
    reply_v3 = reply_v4

class close():
    '''This message is used when the client wants to close the NDMP
        connection'''
    def request_v4(self, record):
        pass
    
    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4


class server_auth():
    '''This optional request is used by the DMA to force the NDMP Server to
        authenticate itself'''

    def request_v4(self, record):
        pass
        
    def reply_v4(self, record):
        record.error = const.NDMP_NOT_SUPPORTED_ERR

    request_v3 = request_v4
    reply_v3 = reply_v4

