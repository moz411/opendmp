import logging.handlers, sys
from tools.config import Config; cfg = Config.cfg

class Log:
    '''Initialize the opendmp logging system.
    Actually print both to stdout and in a LOGFILE defined in opendmp.conf
    '''
    stdlog = logging.getLogger('stdlog')
    
    def getlog(self):
        try:
            numeric_level = getattr(logging, cfg['LOGLEVEL'].upper(), None)
        except:
            print('Invalid log level, using default INFO')
        
        try:
            self.stdlog.setLevel(numeric_level)
        except TypeError:
            print('Invalid log level: ' + cfg['LOGLEVEL'] + ', using default')
            self.cfg['LOGLEVEL'] = 'INFO'
        
        try:
            rh = logging.handlers.RotatingFileHandler(cfg['LOGFILE'], maxBytes=5242880, backupCount=3)
            sh = logging.StreamHandler()
        except IOError:
            print('Cannot write to ' + cfg['LOGFILE'] + ', (are you root?)')
        
        if self.stdlog.isEnabledFor(logging.DEBUG):
            formatter = logging.Formatter(
                '0x%(thread)x: %(message)s'
                )
        else:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s -  %(threadName)s - %(message)s', "%Y-%m-%d %H:%M:%S")
        
        rh.setFormatter(formatter)
        sh.setFormatter(formatter)
        self.stdlog.addHandler(rh)
        self.stdlog.addHandler(sh)
        
        return self.stdlog