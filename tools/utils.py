'''various functions, mainly used to pack NDMP replies'''

import os, sys, re, traceback, time, ctypes
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
import xdr.ndmp_const as const
from xdr.ndmp_type import (ndmp_fs_info_v4, ndmp_u_quad,
                           ndmp_pval, ndmp_device_info_v4,
                           ndmp_device_capability_v3)
   
class Empty():
    pass
  
def list_scsi_hbas(self):
    '''This function returns the list of HBA indexes for existing SCSI HBAs.'''
    if  (c.system == 'Linux'):
        return list(set([int(device.partition(":")[0])
            for device in os.listdir("/sys/bus/scsi/devices")
            if re.match("[0-9:]+", device)]))
    
def list_tapes():
    if  (c.system == 'Linux'):
        return list(set([int(device.partition(":")[0])
            for device in os.listdir("/sys/class_scsi_tape")
            if re.match("nst.+", device)]))

def check_device_opened(record):
    try:
        assert(record.device.opened == True)
        return True
    except (AttributeError, AssertionError):
        record.error = const.NDMP_DEV_NOT_OPEN_ERR
        stdlog.error('device not opened')
        return False
    
def check_device_not_opened(record):
    try:
        assert(record.device.opened == True)
        record.error = const.NDMP_DEVICE_OPENED_ERR
        stdlog.error('device already opened')
        return True
    except (AttributeError, AssertionError):
        return False
        
def add_hctl_linux(record):
    # a full check of all devices is done every time in case the host
    # have reorganized its SCSI devices
    for devtype in ['scsi_generic', 'scsi_tape']:
        devpath = '/sys/class/' + devtype
        try:
            listdir = os.listdir(devpath)
        except OSError:
            stdlog.error('cannot access ' + devpath)
            raise
        if(os.path.basename(record.device.path) in listdir):
            devlink = os.path.join(devpath, os.path.basename(record.device.path))
            if os.path.islink(devlink):
                try:
                    (host, controller, target, lun) = (
                                        re.search('/((?:\d{1,3}:){3}\d{1,3})/',
                                                  os.path.realpath(devlink)).group(1).split(':'))
                except AttributeError:
                    stdlog.error('device ' + record.device.path + ' not found')
                    stdlog.debug(sys.exc_info()[1])
                    raise
    try:
        controller = int(controller)
        target = int(target)
        lun = int(lun)
    except (ValueError, UnboundLocalError):
        stdlog.error('incorrect hctl')
        raise
    return (controller, target, lun)

def add_filesystem_unix(line, local):
    from tools import butypes as bu
    fs = ndmp_fs_info_v4()
    (fs_physical_device, t, fs_logical_device, t, fs_type, options) = line.split()
    (f_bsize, f_frsize, f_blocks, f_bfree, 
     f_bavail, f_files, f_ffree, f_favail, 
     f_flag, f_namemax) = os.statvfs(fs_logical_device)
    avail = repr(','.join(bu.Unix)).encode()
    fs.invalid = 0
    fs.fs_type = fs_type.encode()
    fs.fs_logical_device = fs_logical_device.encode()
    fs.fs_physical_device = fs_physical_device.encode()
    fs.total_size = ndmp_u_quad(high=0,low=f_blocks)
    fs.used_size = ndmp_u_quad(high=0,low=f_blocks-f_bfree)
    fs.avail_size = ndmp_u_quad(high=0,low=f_bfree)
    fs.total_inodes = ndmp_u_quad(high=0,low=f_files)
    fs.used_inodes = ndmp_u_quad(high=0,low=f_files-f_ffree)
    fs.fs_env = [ndmp_pval(name=b'LOCAL', value=local.encode()),
                 ndmp_pval(name=b'TYPE', value=fs_type.encode()),
                 ndmp_pval(name=b'AVAILABLE_BACKUP', value=avail),
                 ndmp_pval(name=b'AVAILABLE_RECOVERY ', value=avail)]
    fs.fs_status = b'online'
    return fs

def add_devinfo_linux(devname):
    dev = ndmp_device_info_v4()
    dev.caplist = []
    capability = ndmp_device_capability_v3()
    capability.capability = []
    try:
        os.access(os.path.join(devname,'device','model'), os.R_OK)
    except OSError:
        stdlog.info('unable to read model of' + devname)
    except:
        stdlog.debug(traceback.print_exc())
    with open(os.path.join(devname,'device','model')) as fp:
        dev.model = fp.read().strip().encode()
    capability.device = b'/dev/' + devname.split('/')[-1].encode()
    capability.attr = 0x0 #(const.NDMP_TAPE_ATTR_REWIND | const.NDMP_TAPE_ATTR_UNLOAD | const.NDMP_TAPE_ATTR_RAW)
    # TODO: send realistic attributes
    capability.capability.append(ndmp_pval(b'EXECUTE_CDB',b'y'))
    dev.caplist.append(capability)
    return dev

class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

class all_ulonglong(ctypes.BigEndianStructure): 
    _fields_ = [ 
                ('high', ctypes.c_ulonglong), 
                ('low', ctypes.c_ulonglong)
                ]
    
    def __repr__(self):
        out = []
        if self.high is not None:
            out += ['high=%s' % repr(self.high)]
        if self.low is not None:
            out += ['low=%s' % repr(self.low)]
        return 'ndmp_u_quad(%s)' % ', '.join(out)

def long_long_to_quad(d):
        h = all_ulonglong()
        h.high = max(0, d >> 32)
        h.low = max(0, d)
        return (h);

def quad_to_long_long(q):
        return ((q.high >> 32) + q.low)


def approximate_size(size, a_kilobyte_is_1024_bytes=True):
    '''From diveintopython3 :-)'''
    SUFFIXES = {1000: ['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
            1024: ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']}
    if size < 0:
        raise ValueError('number must be non-negative')
    multiple = 1024 if a_kilobyte_is_1024_bytes else 1000
    for suffix in SUFFIXES[multiple]:
        size /= multiple
        if size < multiple:
            return '{0:.1f} {1}'.format(size, suffix)
    raise ValueError('number too large')