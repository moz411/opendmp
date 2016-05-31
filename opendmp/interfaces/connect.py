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

from tools.config import Config; cfg = Config.cfg
from tools.log import Log; stdlog = Log.stdlog
import hashlib
import xdr.ndmp_const as const
from xdr.ndmp_type import ndmp_auth_data, ndmp_auth_md5


class open():
    '''This message negotiates the protocol version to be used between the
        DMA and NDMP Server. '''
    
    async def request_v4(self, record):
        record.protocol_version = 'v' + repr(record.b.protocol_version)
        

    async def reply_v4(self, record):
        if(record.protocol_version in cfg['SUPPORTED_NDMP_VERSIONS']):
            record.error = const.NDMP_NO_ERR
        else:
            record.error =  const.NDMP_ILLEGAL_ARGS_ERR
    
    request_v3 = request_v4
    reply_v3 = reply_v4


class client_auth():
    '''This request authenticates the DMA to a NDMP Server.'''
    
    async def request_v4(self, record):
        record.auth = record.b

    async def reply_v4(self, record):
        record.auth = None

    request_v3 = request_v4
    reply_v3 = reply_v4


class server_auth():
    '''This optional request is used by the DMA to force the NDMP Server to
        authenticate itself'''

    async def request_v4(self, record):
        try:
            assert(record.b.client_attr.auth_type == const.NDMP_AUTH_MD5)
            record.challenge = record.b.client_attr.challenge
        except(AssertionError):
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
        
    async def reply_v4(self, record):
        # TODO: still not working with ndmfs test
        m = hashlib.md5()
        password = cfg['PASSWORD'].encode()
        m.update(record.challenge + password)
        record.b.auth_result = ndmp_auth_data
        record.b.auth_result.auth_type = const.NDMP_AUTH_MD5
        record.b.auth_result.auth_md5 = ndmp_auth_md5
        record.b.auth_result.auth_md5.user = cfg['USER'].encode()
        record.b.auth_result.auth_md5.auth_digest = m.digest()

    request_v3 = request_v4
    reply_v3 = reply_v4

class close():
    '''This message is used when the client wants to close the NDMP
        connection'''
    async def request_v4(self, record):
        pass
    
    async def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4