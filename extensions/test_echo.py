'''NDMP Extensions Test Message 
   The extension class 0x7ff0, interface 00, message 00 will be used as 
   a extension test message. All NDMP servers that implement extensions 
   SHOULD also implement the test message. The DMA and server 
   implementer can use this message as a vehicle for testing their 
   implementation of extensions discovery and negotiation, as well as 
   error handling. In order to test the discovery and negotiation 
   process, two versions of the 0x7ff0 class with different message 
   definitions will be defined. '''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config 
import xdr.ndmp_const as const
 
class ndmp_test_echo_v1():
    
    def get_class_id(self):
        return '0x7ff0'
    def get_version(self):
        return '1'
    
    def reply(self, record):
        pass

class ndmp_test_echo_v2():
    
    def get_class_id(self):
        return '0x7ff0'
    def get_version(self):
        return '2'
    
    def reply(self, record):
        pass
