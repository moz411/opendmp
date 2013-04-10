import configparser, os, platform, socket
from subprocess import Popen, PIPE
import sys

class Config:
    ''' Read and interpret the opendmp configuration file.
    If configuration file is corrupted or unavailable, use defaults
    '''
    __version__ = '0.1.0'
    vendor_name = 'GNU'
    product_name = 'opendmp'
    (system, hostname, release, version, machine, processor) = platform.uname()
    (hostname, aliaslist, addresslist) = socket.gethostbyaddr(socket.gethostname())
    h = Popen('hostid', stdout=PIPE)
    h.wait()
    hostid = h.stdout.read()
    Unix = ['Linux','SunOS','AIX','HP-UX','Tru64','FreeBSD','OpenBSD','NetBSD']
    Windows = ['NT']
    b = configparser.ConfigParser()
        
    b['DEFAULT'] = {'HOST': '0.0.0.0',
                    'PORT': '10000',
                    'SOCKET_TIMEOUT': '50',
                    'MAX_THREADS': '0',
                    'LOGLEVEL': 'INFO',
                    'LOGFILE': '/var/log/opendmp.log',
                    'PREFERRED_NDMP_VERSION' : 'v4',
                    'SUPPORTED_NDMP_VERSIONS': ['v4','v3'],
                    'BUFSIZE': '10240',
                    'DATA_PORT_RANGE': '10001-10020',
                    'DATA_TIMEOUT': '300'
                    }
    
    cfg = b['DEFAULT']
    
    def __init__(self, cfgfile):
        self.cfgfile = cfgfile

    def getcfg(self):
        ''' Read and verify the configuration file give in argument'''
        c = configparser.ConfigParser()
        try:
            if not c.read(self.cfgfile):
                raise IOError()
            else:
                self.b.update(c)
        except IOError:
            print('Cannot read ' + self.cfgfile +', using defaults', file=sys.stderr)  
        
        try:
            int(self.cfg['PORT'])
        except ValueError:
            print('Invalid value given for PORT, using default 10000', file=sys.stderr)
            self.cfg['PORT'] = '10000'
        
        if int(self.cfg['PORT']) < 1024 or int(self.cfg['PORT']) > 65535:
            print('Invalid value given for PORT, using default 10000', file=sys.stderr)
            self.cfg['PORT'] = '10000'
            
        return self.cfg