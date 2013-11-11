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
                        'DATA_TIMEOUT': '5',
                        'DUMPDATES': '/etc/dumpdates',
                        'EMULATE_NETAPP': 'False',
                        'EMULATE_CELERRA': 'False',
                        'EMULATE_ISILON': 'False'
                        }
    cfg = config['DEFAULT']
    threads = []
    
    (system, hostname, release, version, machine, processor) = platform.uname()
    (hostname, aliaslist, addresslist) = socket.gethostbyaddr(socket.gethostname())
    
    vendor_name = 'GNU'
    product_name = 'opendmp'
    revision_number = '1.0'
    os_type = system
    os_vers = release
        
        
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
        print(self)
        if (self.cfg['EMULATE_NETAPP'] == 'True'):
            self.vendor_name = 'Netapp'
            self.product_name = 'Super Filer'
            self.revision_number = '8.0R1'
            self.os_type = 'Netapp'
            self.os_vers = '8.0R1'
        elif (self.cfg['EMULATE_CELERRA'] == 'True'):
            self.vendor_name = 'EMC'
            self.product_name = 'CELERRA'
            self.revision_number = 'T.7.1.65.8'
            self.os_type = 'CELERRA'
            self.os_vers = 'T.7.1.65.8'
        elif (self.cfg['EMULATE_ISILON'] == 'True'):
            self.vendor_name = 'ISILON'
            self.product_name = 'OneFS'
            self.revision_number = '7.0.0'
            self.os_type = 'OneFS'
            self.os_vers = '7.0.0'
        
        return self.cfg