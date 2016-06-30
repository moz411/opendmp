'''various functions, mainly used to pack NDMP replies'''

import os, sys, re, traceback, time, ctypes, binascii, pickle, errno
from functools import wraps
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const, ndmp_type as types
from xdr.ndmp_pack import NDMPPacker
from xdr.ndmp_type import (ndmp_fs_info_v4, ndmp_u_quad,
                           ndmp_pval, ndmp_device_info_v4,
                           ndmp_device_capability_v3)
import inspect
from importlib.util import find_spec, module_from_spec
   
class Empty():
    pass
  
def list_scsi_hbas(self):
    '''This function returns the list of HBA indexes for existing SCSI HBAs.'''
    if  (c.system == 'Linux'):
        return list(set([int(device.partition(":")[0])
            for device in os.listdir("/sys/bus/scsi/devices")
            if re.match("[0-9:]+", device)]))
    
def list_devices():
    devices = []
    if  (c.system == 'Linux'):
        try:
            for dev in os.listdir('/sys/class/scsi_tape'):
                devices.append('/dev/' + dev)
        except FileNotFoundError: # Not tape drives
            pass
        try:
            for devname in (os.listdir('/sys/class/scsi_changer')): # ndmp_device_info
                    path = os.path.join('/sys/class/scsi_changer',devname,'device/scsi_generic')
                    for generic in (os.listdir(path)):
                        devices.append('/dev/' + generic)
        except FileNotFoundError: # Not changers
            pass
    return devices

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

def add_filesystem_unix(line, local, plugins):
    fs = ndmp_fs_info_v4()
    result = line.split()
    fs_physical_device = result[0]
    fs_logical_device = result[2]
    if(c.system == 'Linux'): 
        fs_type = result[4]
    else:
        fs_type = re.sub('\(|,', '', result[3])
    try:
        (f_bsize, f_frsize, f_blocks, f_bfree, 
         f_bavail, f_files, f_ffree, f_favail, 
         f_flag, f_namemax) = os.statvfs(fs_logical_device)
    except OSError as e:
        stdlog.error(e)
        return None
    if (cfg['EMULATE_NETAPP'] == 'True'):
        avail = b'dump'
    else:
        avail = []
        for bu in plugins:
            if c.system in bu.ostype:
                avail.append(bu.butype_info.butype_name)
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
                 ndmp_pval(name=b'AVAILABLE_BACKUP', value=b','.join(avail)),
                 ndmp_pval(name=b'AVAILABLE_RECOVERY', value=b','.join(avail))]
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

def long_long_to_quad(d):
    u_quad = ndmp_u_quad()
    u_quad.high, u_quad.low = divmod(d, 1<<32)
    return u_quad
        

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

def give_fifo(file=None):
    if not file:
        file = bytes(binascii.b2a_hex(os.urandom(5))).decode()
    filename = os.path.join(cfg['RUNDIR'], file)
    try:
        assert os.path.abspath(filename).startswith(cfg['RUNDIR'])
        os.mkfifo(filename, mode=0o600)
    except AssertionError as e:
        stdlog.error(e)
    else:
        return filename

def clean_fifo(file):
    filename = os.path.join(cfg['RUNDIR'], file)
    try:
        assert os.path.abspath(filename).startswith(cfg['RUNDIR'])
        os.remove(filename)
    except (AssertionError, OSError) as e:
        stdlog.error(e)

        
def check_file_mode(st_mode):
    if (st_mode == 10):
        ftype =  const.NDMP_FILE_REG
    elif (st_mode == 4):
        ftype =  const.NDMP_FILE_DIR
    elif(st_mode == 12):
        ftype =  const.NDMP_FILE_SLINK
    elif(st_mode == 1):
        ftype =  const.NDMP_FILE_FIFO
    elif(st_mode == 6):
        ftype =  const.NDMP_FILE_BSPEC
    elif(st_mode == 2):
        ftype =  const.NDMP_FILE_CSPEC
    elif(st_mode == 14):
        ftype =  const.NDMP_FILE_SOCK
    else:
        ftype =  const.NDMP_FILE_OTHER
    return (ftype)

def check_mode_file(st_mode):
    if (st_mode == '-'):
        ftype =  const.NDMP_FILE_REG
    elif (st_mode == 'd'):
        ftype =  const.NDMP_FILE_DIR
    elif(st_mode == 'l'):
        ftype =  const.NDMP_FILE_SLINK
    elif(st_mode == 'f'):
        ftype =  const.NDMP_FILE_FIFO
    elif(st_mode == 'b'):
        ftype =  const.NDMP_FILE_BSPEC
    elif(st_mode == 'c'):
        ftype =  const.NDMP_FILE_CSPEC
    elif(st_mode == 's'):
        ftype =  const.NDMP_FILE_SOCK
    else:
        ftype =  const.NDMP_FILE_OTHER
    return (ftype)

def touchopen(filename, mode='a'):
    try:
        open(filename, "a").close() # "touch" file
    except OSError as e:
        stdlog.error(e)
        raise
    else:
        return open(filename, mode)

def read_dumpdates(file):
    # Read dumpdates or create it
    with touchopen(file, 'rb') as dumpdates:
        return pickle.load(dumpdates)

def write_dumpdates(file, dumpdates):
    with open(file, 'wb+') as dumpfile:
        pickle.dump(dumpdates, dumpfile, pickle.HIGHEST_PROTOCOL)
 
def mktstampfile(file, tstamp):
    with open(file, 'w') as node:
        pass
    os.utime(file,times=(tstamp,tstamp))
    return (file)

def compute_incremental(dumpdates, filesystem, level):
    tstamp = None
    while(tstamp == None and level > 0):
        try:
            tstamp = [dumpdates[key] for key in dumpdates.keys() 
                      if key[0] == filesystem and 
                         key[1] == level][0]
        except IndexError:
            level-=1
            continue
    return tstamp
    
def valid_state(state, reverse=True):
    ''' A decorator that validate the Mover or Data session state '''
    if isinstance(state, int):
        state = [state]
        
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            record = args[1]
            if func.__module__ == 'interfaces.data':
                record_state = record.data['state']
            elif func.__module__ == 'interfaces.mover':
                record_state = record.mover['state']
            if ((reverse and record_state not in state) 
                or (not reverse and record_state in state)):
                record.error = const.NDMP_ILLEGAL_STATE_ERR
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorate

def post(body_pack_func, message):
    '''
    A decorator that prepare the message post header, 
    then execute post function, pack and enqueue the message
    '''
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            record = args[1]
            p = NDMPPacker()
            # Header
            record.post_header = types.ndmp_header()
            record.post_header.message_type = const.NDMP_MESSAGE_REQUEST
            record.post_header.message = message
            record.post_header.sequence = record.server_sequence
            record.post_header.reply_sequence = 0
            record.post_header.time_stamp = int(time.time())
            record.post_header.error = const.NDMP_NO_ERR
            record.post_body = find_interface('xdr.ndmp_type',body_pack_func)
            yield from func(*args, **kwargs)
            p.pack_ndmp_header(record.post_header)
            mymodule = ([obj for cl, obj in inspect.getmembers(p) if cl == 'pack_'+body_pack_func])[0]
            mymodule(record.post_body)
            record.server_sequence+=1
            stdlog.debug(repr(record.post_body))
            record.ndmpserver.handle_write(p.get_buffer())
            if(record.bu['bu']): record.bu['bu'].history.clear()
        return wrapper
    return decorate

def opened(reversed=False):
    ''' A decorator that validate if a device is opened '''
    def decorate(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            record = args[1]
            if not record.device['path']:
                record.device['path'] record.b.device.decode()
            if record.device['path'] not in list_devices():
                stdlog.error('Access to device ' + record.device['path'] + ' not allowed')
                record.error = const.NDMP_NOT_AUTHORIZED_ERR
            elif (reversed and not record.device['opened']):
                
            else:
                await func(*args, **kwargs)
        return wrapper
    return decorate

def try_io(func):
    '''
    A decorator that encapsulate the os.io operations
    and catch os errors
    '''
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            record = args[1]
            try:
                yield from func(*args, **kwargs)
            except (OSError, IOError) as e:
                stdlog.error(e.strerror)
                if(e.errno == errno.EACCES):
                    record.error = const.NDMP_WRITE_PROTECT_ERR
                elif(e.errno == errno.ENOENT):
                    record.error = const.NDMP_NO_DEVICE_ERR
                elif(e.errno == errno.EBUSY):
                    record.error = const.NDMP_DEVICE_BUSY_ERR
                elif(e.errno == errno.ENODEV):
                    record.error = const.NDMP_NO_DEVICE_ERR
                elif(e.errno == errno.ENOSPC):
                    record.error = const.NDMP_EOM_ERR
                    #record.mover['pause_reason'] = const.NDMP_MOVER_PAUSE_EOM
                    #record.mover['state'] = const.NDMP_MOVER_STATE_PAUSED
                    #try:
                    #    record.device.fd.flush()
                    #except BlockingIOError as e:
                    #    stdlog.debug(e)
                elif(e.errno == 123):
                    record.error = const.NDMP_NO_TAPE_LOADED_ERR
                else:
                    record.error = const.NDMP_IO_ERR
        return wrapper
    return decorate
 
def find_interface(package, name, func=None):
    '''Find the module to import corresponding to the message
    Create an instance of the parent class in interfaces package'''
    spec = find_spec(package)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    myclass = ([obj for cl, obj in inspect.getmembers(module) if cl == name])[0]
    inst = myclass()
    if func: 
        mymodule = ([obj for cl, obj in inspect.getmembers(myclass) if cl == func])[0]
        return(inst, mymodule)
    else:
        return(inst)