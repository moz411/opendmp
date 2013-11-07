import threading, logging.handlers, sys
from server.config import Config; cfg = Config.cfg

class Log(threading.Thread):
    '''Initialize the opendmp logging system.
    Actually print both to stdout and in a LOGFILE defined in opendmp.conf
    '''
    
    
    def __init__(self, record):
        threading.Thread.__init__(self, name='Log-' + repr(threading.activeCount()))
        self.stdlog = logging.getLogger('stdlog')
        self.record = record
        
    def run(self):
            sys.exit()
    
