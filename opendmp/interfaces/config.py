'''This interface allows the DMA to discover the configuration of the  NDMP Server.'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config 
import os, re, traceback
from subprocess import Popen, PIPE
import xdr.ndmp_const as const
import tools.utils as ut
from xdr.ndmp_type import (ndmp_auth_attr, ndmp_class_list)

class get_host_info():
    '''This request is used to get information about the host on which the NDMP Server is running.'''
    
    def reply_v4(self, record):
        record.b.os_type = c.os_type.encode()
        record.b.os_vers = c.os_vers.encode()
        record.b.hostid = c.hostid
        record.b.hostname = c.hostname.encode()
        record.b.auth_type = [const.NDMP_AUTH_MD5]
        
    reply_v3 = reply_v4
        
class get_server_info():
    '''This request is used to get information about the NDMP Server implementation.'''
    
    def reply_v4(self, record):
        if(record.connected):
            record.b.vendor_name = c.vendor_name.encode()
            record.b.product_name = c.product_name.encode()
            record.b.revision_number = c.revision_number.encode()
        else:
            record.b.vendor_name = b''
            record.b.product_name = b''
            record.b.revision_number = b''
        record.b.auth_type = [const.NDMP_AUTH_MD5]

    reply_v3 = reply_v4

class get_connection_type():
    '''This request returns a list of the data connection types supported by the NDMP Server.'''
      
    def reply_v4(self, record):
        #record.b.addr_types = [const.NDMP_ADDR_LOCAL, const.NDMP_ADDR_TCP, const.NDMP_ADDR_IPC]
        record.b.addr_types = [const.NDMP_ADDR_LOCAL, const.NDMP_ADDR_TCP]

    reply_v3 = reply_v4

class get_auth_attr():
    '''This message is used by the DMA to obtain the attributes of the
        authentication methods supported by the server.'''
    def request_v4(self, record):
        pass
        
    def reply_v4(self, record):
        record.b.server_attr = ndmp_auth_attr(const.NDMP_AUTH_MD5, record.challenge)
    
    request_v3 = request_v4    
    reply_v3 = reply_v4
        
        
class get_butype_info():
    '''This message is used to query the backup types supported by the NDMP
        Server and the capability of each supported backup type.'''
    
    def reply_v4(self, record):
        # TODO: implement a real plugin system
        
        record.b.butype_info = []
        if(c.system in c.Unix):
            from bu import tar as bu
            info = bu.info
            if (cfg['EMULATE_NETAPP'] == 'True'):
                info.butype_name = b'dump'
            record.b.butype_info.append(bu.info)
        elif(c.system == 'Windows'):
            if(c.release >= 7):
                record.b.butype_info.append(bu.wbadmin)
            else:
                record.b.butype_info.append(bu.ntbackup)
        
    def mask(self, v):
        return bin(v <<32)
    
    reply_v3 = reply_v4

class get_fs_info():
    '''This message is used to query information about the file systems on
        the NDMP Server host.'''
    
    def reply_v4(self, record):
        record.b.fs_info = []
        if(c.system in c.Unix):
            try:
                lines, stderr = Popen(['mount','-t','zfs,ufs,gfs,reiserfs,ext2,ext3,ext4'], 
                                  stdout=PIPE, stderr=PIPE).communicate()
                for line in lines.splitlines():
                    fs = ut.add_filesystem_unix(line.decode(), local='y') # local fs
                    if fs: record.b.fs_info.append(fs)
                lines, stderr = Popen(['mount','-t','nfs,smbfs,cifs,vboxsf,vmfs,fuse'], 
                              stdout=PIPE, stderr=PIPE).communicate()
                for line in lines.splitlines():
                    fs = ut.add_filesystem_unix(line.decode(), local='n') # remote fs
                    if fs: record.b.fs_info.append(fs)
            except OSError:
                stdlog.error('[%d] ' + stderr, record.fileno)
                stdlog.debug(traceback.print_exc())
                record.error = const.NDMP_NOT_SUPPORTED_ERR
                
    reply_v3 = reply_v4

class get_tape_info():
    '''This message is used to query information about the tape devices
               connected to the NDMP Server host.'''
    
    def reply_v4(self, record):
        record.b.tape_info = []
        
        if(c.system == 'Linux'):
            scsi_tape_path = '/sys/class/scsi_tape'
            try: os.access(scsi_tape_path, os.R_OK) # there are tape drives available
            except OSError: 
                stdlog.info('[%d] No tape device found', record.fileno)
                
            for devname in os.listdir(scsi_tape_path):
                if (re.match('nst[0-9]+$',devname)):
                    devinfo = ut.add_devinfo_linux(os.path.join(scsi_tape_path,devname))
                    record.b.tape_info.append(devinfo)
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR

    reply_v3 = reply_v4

class get_scsi_info():
    '''This message is used to query information about the SCSI media
               changer devices connected to the NDMP Server host.'''
    
    def reply_v4(self, record):
        record.b.scsi_info = []
        
        if(c.system == 'Linux'):
            scsi_changer_path = '/sys/class/scsi_changer'
            try:
                os.access(scsi_changer_path, os.R_OK)
            except OSError:
                stdlog.info('[%d] No changer found', record.fileno)
            for devname in (os.listdir(scsi_changer_path)): # ndmp_device_info
                path = os.path.join(scsi_changer_path,devname,'device/scsi_generic')
                for generic in (os.listdir(path)):
                    devinfo = ut.add_devinfo_linux(os.path.join(path,generic))
                record.b.scsi_info.append(devinfo)
        else:
            record.error = const.NDMP_NOT_SUPPORTED_ERR

    reply_v3 = reply_v4

class get_ext_list():
    '''NDMP_CONFIG_GET_EXT_LIST is used to request which classes of
       extensions and versions are available.'''
    def reply_v4(self, record):
        
        # TODO: improve this very simple module loader
        classlist = ndmp_class_list()
        record.b.class_list = []
        curdir = os.path.abspath(os.path.dirname(__file__))
        extdir = os.path.normpath(os.path.join(curdir,'..','extensions'))
        for module in list(modules for modules in os.listdir(extdir) 
                           if re.match('[^_][a-zA-Z0-9_]+\\.py', modules)):
            mod = re.split('\.', module)[0]
            try:
                exec('import extensions.'+ mod)
            except ImportError as e:
                stdlog.error('[%d] Unable to load extension ' + 
                             mod + ': ' + e, record.fileno)
                next
            #exec('for extension in list(extensions for extensions in dir(extensions.' + mod ') if re.match(\'ndmp+\', extensions)):')
            #    print(extension)
                                
            classlist.ext_class_id = 0x7ff0 #Class 0x7ff0 V1/2 Echo Interface
            classlist.ext_version = [1]
            record.b.class_list.append(classlist)
        #record.error = const.NDMP_NOT_SUPPORTED_ERR
        #TODO: use reserved class 21E0.0000  21E3.FFFF

    reply_v3 = reply_v4

class set_ext_list():
    '''After a successful reply to the NDMP_CONFIG_GET_EXT_LIST the DMA
       SHOULD issue a NDMP_CONFIG_SET_EXT_LIST request to select which
       extensions, and which version of each extension it will use.'''
    def request_v4(self, record):
        pass
    
    def reply_v4(self, record):
        record.error = const.NDMP_CLASS_NOT_SUPPORTED_ERR

    request_v3 = request_v4
    reply_v3 = reply_v4