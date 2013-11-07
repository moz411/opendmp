import sys, os, configparser, platform, socket, traceback

class Config:
    ''' Read and interpret the opendmp configuration file.
    If configuration file is corrupted or unavailable, use defaults
    '''
    config = configparser.ConfigParser()
    config['DEFAULT'] = {'HOST': '0.0.0.0',
                        'PORT': '10000',
                        'SOCKET_TIMEOUT': '50',
                        'MAX_THREADS': '0',
                        'LOGLEVEL': 'INFO',
                        'LOGFILE': '/var/log/opendmp.log',
                        'RUNDIR': '/var/run/opendmp',
                        'PREFERRED_NDMP_VERSION' : 'v4',
                        'SUPPORTED_NDMP_VERSIONS': ['v4','v3'],
                        'BUFSIZE': '10240',
                        'DATA_PORT_RANGE': '10001-10020',
                        'DATA_TIMEOUT': '300',
                        'DUMPDATES': '/etc/dumpdates',
                        'EMULATE_NETAPP': 'False',
                        'EMULATE_CELERRA': 'False'
                        }
    cfg = config['DEFAULT']
    threads = []
    
    __version__ = '0.1.0'
    vendor_name = 'GNU'
    product_name = 'opendmp'
    (system, hostname, release, version, machine, processor) = platform.uname()
    (hostname, aliaslist, addresslist) = socket.gethostbyaddr(socket.gethostname())
    if(system in ['FreeBSD', 'OpenBSD','NetBSD']):
        hostid = os.popen('/sbin/sysctl -n kern.hostuuid').readlines()[0].encode()
    else:
        hostid = os.popen('hostid').readlines()[0].encode()
    Unix = ['Linux','SunOS','AIX','HP-UX','Tru64','FreeBSD','OpenBSD','NetBSD']
    Windows = ['NT']

    def getcfg(self, cfgfile):
        ''' Read and verify the configuration file give in argument'''
        try:
            self.config.read(os.path.normpath(os.path.join(
                                                      os.path.abspath(os.path.dirname(__file__)),
                                                      '..',
                                                      cfgfile)
                                         ))
            self.cfg.update(self.config['DEFAULT'])
        except:
            print(traceback.format_exc())
            print('Error getting configuration, using defaults')
        try:
            int(self.cfg['PORT'])
        except (KeyError, ValueError):
            print('Invalid value given for PORT, using default 10000')
            self.cfg['PORT'] = '10000'
        
        if int(self.cfg['PORT']) < 1024 or int(self.cfg['PORT']) > 65535:
            print('Invalid value given for PORT, using default 10000')
            self.cfg['PORT'] = '10000'
            
        return self.cfg