'''
From linux/mtio.h header file for Linux. Written by H. Bergman
Structures and definitions for mag tape io control commands
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import xdr.ndmp_const as const
from tools import utils as ut
from ctypes import *
from fcntl import ioctl
from os import errno

# mag tape io control commands
def MTIOCTOP(record):
    '''do a mag tape op'''
    if(record.b.tape_op == const.NDMP_MTIO_REW):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_OFF):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_TUR):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_FSF):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_BSF):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_FSR):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_BSR):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_REW):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_EOF):
        pass
    elif(record.b.tape_op == const.NDMP_MTIO_OFF):
        pass
    else:
        record.error = const.NDMP_NOT_SUPPORTED_ERR
        return
    
    buf = create_string_buffer(sizeof(MTOP))
    memset(buf, 0, sizeof(MTOP))
    mtop = MTOP.from_buffer(buf)
    mtop.mt_op = mt_opcodes[record.b.tape_op]
    mtop.mt_count = record.b.count
    io = _IOW('m', 1, mtop)
    try:
        ioctl(record.device.fd, io, mtop)
    except (OSError, IOError) as e:
        stdlog.error(const.ndmp_tape_mtio_op[record.b.tape_op] + 
                     ' failed for ' + record.device.path + ': ' + e.strerror)
        if(e.errno == 123): # No medium
            record.error = const.NDMP_NO_TAPE_LOADED_ERR
        elif(e.errno == errno.EACCES):
            record.error = const.NDMP_WRITE_PROTECT_ERR
        else:
            record.error = const.NDMP_IO_ERR
    return mtop

def MTIOCGET(record):
    '''get tape status'''
    mt = {}
    buf = create_string_buffer(sizeof(MTGET))
    memset(buf, 0, sizeof(MTGET))
    mtget = MTGET.from_buffer(buf)
    io = _IOR('m', 2, mtget)
    try:
        ioctl(record.device.fd, io, mtget)
    except (OSError, IOError) as e:
        if(e.errno == 123): # No medium
            record.error = const.NDMP_NO_TAPE_LOADED_ERR
        elif(e.errno == errno.EACCES):
            record.error = const.NDMP_WRITE_PROTECT_ERR
        stdlog.error('mtio GET failed for ' + record.device.path + ': ' + e.strerror)
    except Exception as e:
        record.error = const.NDMP_IO_ERR
        stdlog.error('mtio GET failed for ' + record.device.path + ': ' + e.strerror)
    mt['unsupported'] = const.NDMP_TAPE_STATE_TOTAL_SPACE_INVALID | const.NDMP_TAPE_STATE_SPACE_REMAIN_INVALID
    if(bool(GMT_WR_PROT(mtget.mt_gstat))):
        mt['flags'] = const.NDMP_TAPE_STATE_WR_PROT
        record.error = const.NDMP_WRITE_PROTECT_ERR
    else:
        mt['flags'] = 0
        
    mt['resid'] = mtget.mt_resid
    mt['file_num'] = mtget.mt_fileno
    mt['soft_errors'] = _IOC_SOFTERR(mtget.mt_erreg) # decode MT_ST_SOFTERR_SHIFT and MT_ST_SOFTERR_MASK
    mt['block_size'] = _IOC_BLKSIZE(mtget.mt_dsreg) # decode MT_ST_BLKSIZE_SHIFT, MT_ST_BLKSIZE_MASK
    mt['blockno'] = mtget.mt_blkno
    mt['total_space'] = ut.long_long_to_quad(1*64)
    mt['space_remain'] = ut.long_long_to_quad(1*64)
    return mt
    
def MTIOCPOS(record):
    '''get tape position'''
    buf = create_string_buffer(sizeof(MTPOS))
    memset(buf, 0, sizeof(MTPOS))
    mtpos = MTPOS.from_buffer(buf)
    mtpos.lmt_blkno = record.device.mt['blkno']
    io = _IOR('m', 3, mtpos)
    ioctl(record.device.fd, io, mtpos)
    record.device.mtio = mtpos

class MTOP(Structure):
    '''structure for MTIOCTOP - mag tape op command '''
    _fields_ = [
        ("mt_op", c_short), # operations defined below
        ("mt_count", c_int)] # how many of them
    
class MTGET(Structure):
    '''structure for MTIOCTOP - mag tape op command '''
    _fields_ = [
        ("mt_type", c_long), # type of magtape device
        ("mt_resid", c_long), # residual count: (not sure)
                                # number of bytes ignored, or
                                # number of files not skipped, or
                                # number of records not skipped.
        # the following registers are device dependent
        ("mt_dsreg", c_long), # status register 
        ("mt_gstat", c_long), # generic (device independent) status
        ("mt_erreg", c_long), # error register
        # The next two fields are not always used
        ("mt_fileno", c_int), # __kernel_daddr_t number of current file on tape
        ("mt_blkno", c_int)] # __kernel_daddr_t current block number

class MTPOS(Structure):
    '''structure for MTIOCPOS - mag tape get position command'''
    _fields_ = [
        ("lmt_blkno", c_long)] # current block number

'''Magnetic Tape operations [Not all operations supported by all drivers]:'''
    
MTRESET         = 0    # +reset drive in case of problems 
MTFSF           = 1    # forward space over FileMark, position at first record of next file 
MTBSF           = 2    # backward space FileMark (position before FM) 
MTFSR           = 3    # forward space record 
MTBSR           = 4    # backward space record 
MTWEOF          = 5    # write an end-of-file record (mark) 
MTREW           = 6    # rewind 
MTOFFL          = 7    # rewind and put the drive offline (eject?) 
MTNOP           = 8    # no op, set status only (read with MTIOCGET) 
MTRETEN         = 9    # retension tape 
MTBSFM          = 10    # +backward space FileMark, position at FM 
MTFSFM          = 11    # +forward space FileMark, position at FM 
MTEOM           = 12    # goto end of recorded media (for appending files).
                # MTEOM positions after the last FM, ready for
                # appending another file.
MTERASE         = 13    # erase tape -- be careful! 
MTRAS1          = 14    # run self test 1 (nondestructive) 
MTRAS2          = 15    # run self test 2 (destructive) 
MTRAS3          = 16    # reserved for self test 3 
MTSETBLK        = 20    # set block length (SCSI) 
MTSETDENSITY    = 21    # set tape density (SCSI) 
MTSEEK          = 22    # seek to block (Tandberg, etc.) 
MTTELL          = 23    # tell block (Tandberg, etc.) 
MTSETDRVBUFFER  = 24 # set the drive buffering according to SCSI-2 
            # ordinary buffered operation with code 1 
MTFSS           = 25    # space forward over setmarks 
MTBSS           = 26    # space backward over setmarks 
MTWSM           = 27    # write setmarks 
MTLOCK          = 28    # lock the drive door 
MTUNLOCK        = 29    # unlock the drive door 
MTLOAD          = 30    # execute the SCSI load command 
MTUNLOAD        = 31    # execute the SCSI unload command 
MTCOMPRESSION   = 32# control compression with SCSI mode page 15 
MTSETPART       = 33    # Change the active tape partition 
MTMKPART        = 34    # Format the tape with one or two partitions 
MTWEOFI         = 35    # write an end-of-file record (mark) in immediate mode 

'''Constants for mt_type. Not all of these are supported,
and these are not all of the ones that are supported.'''
MT_ISUNKNOWN            = 0x01
MT_ISQIC02              = 0x02    # Generic QIC-02 tape streamer */
MT_ISWT5150             = 0x03    # Wangtek 5150EQ, QIC-150, QIC-02 */
MT_ISARCHIVE_5945L2     = 0x04    # Archive 5945L-2, QIC-24, QIC-02? */
MT_ISCMSJ500            = 0x05    # CMS Jumbo 500 (QIC-02?) */
MT_ISTDC3610            = 0x06    # Tandberg 6310, QIC-24 */
MT_ISARCHIVE_VP60I      = 0x07    # Archive VP60i, QIC-02 */
MT_ISARCHIVE_2150L      = 0x08    # Archive Viper 2150L */
MT_ISARCHIVE_2060L      = 0x09    # Archive Viper 2060L */
MT_ISARCHIVESC499       = 0x0A    # Archive SC-499 QIC-36 controller */
MT_ISQIC02_ALL_FEATURES = 0x0F    # Generic QIC-02 with all features */
MT_ISWT5099EEN24        = 0x11    # Wangtek 5099-een24, 60MB, QIC-24 */
MT_ISTEAC_MT2ST         = 0x12    # Teac MT-2ST 155mb drive, Teac DC-1 card (Wangtek type) */
MT_ISEVEREX_FT40A       = 0x32    # Everex FT40A (QIC-40) */
MT_ISDDS1               = 0x51    # DDS device without partitions */
MT_ISDDS2               = 0x52    # DDS device with partitions */
MT_ISONSTREAM_SC        = 0x61   # OnStream SCSI tape drives (SC-x0) and SCSI emulated (DI, DP, USB) */
MT_ISSCSI1              = 0x71    # Generic ANSI SCSI-1 tape unit */
MT_ISSCSI2              = 0x72    # Generic ANSI SCSI-2 tape unit */
# QIC-40/80/3010/3020 ftape supported drives.
# 20bit vendor ID + 0x800000 (see ftape-vendors.h)
MT_ISFTAPE_UNKNOWN      = 0x800000 # obsolete */
MT_ISFTAPE_FLAG         = 0x800000

'''Generic Mag Tape (device independent) status macros for examining
    mt_gstat -- HP-UX compatible.
    There is room for more generic status bits here, but I don't
    know which of them are reserved. At least three or so should
    be added to make this really useful.
'''
GMT_EOF         = lambda x : ((x) & 0x80000000)
GMT_BOT         = lambda x : ((x) & 0x40000000)
GMT_EOT         = lambda x : ((x) & 0x20000000)
GMT_SM          = lambda x : ((x) & 0x10000000)  # DDS setmark */
GMT_EOD         = lambda x : ((x) & 0x08000000)  # DDS EOD */
GMT_WR_PROT     = lambda x : ((x) & 0x04000000)
# GMT_ ?         ((x) & 0x02000000) */
GMT_ONLINE      = lambda x : ((x) & 0x01000000)
GMT_D_6250      = lambda x : ((x) & 0x00800000)
GMT_D_1600      = lambda x : ((x) & 0x00400000)
GMT_D_800       = lambda x : ((x) & 0x00200000)
# GMT_ ?         ((x) & 0x00100000) */
# GMT_ ?         ((x) & 0x00080000) */
GMT_DR_OPEN     = lambda x : ((x) & 0x00040000)  # door open (no tape) */
# GMT_ ?         ((x) & 0x00020000) */
GMT_IM_REP_EN   = lambda x : ((x) & 0x00010000)  # immediate report mode */
GMT_CLN         = lambda x : ((x) & 0x00008000)  # cleaning requested */
# 15 generic status bits unused */

# SCSI-tape specific definitions */
# Bitfield shifts in the status  */
MT_ST_BLKSIZE_SHIFT     = 0
MT_ST_BLKSIZE_MASK      = 0xffffff
MT_ST_DENSITY_SHIFT     = 24
MT_ST_DENSITY_MASK      = 0xff000000
MT_ST_SOFTERR_SHIFT     = 0
MT_ST_SOFTERR_MASK      = 0xffff

# Bitfields for the MTSETDRVBUFFER ioctl */
MT_ST_OPTIONS           = 0xf0000000
MT_ST_BOOLEANS          = 0x10000000
MT_ST_SETBOOLEANS       = 0x30000000
MT_ST_CLEARBOOLEANS     = 0x40000000
MT_ST_WRITE_THRESHOLD   = 0x20000000
MT_ST_DEF_BLKSIZE       = 0x50000000
MT_ST_DEF_OPTIONS       = 0x60000000
MT_ST_TIMEOUTS          = 0x70000000
MT_ST_SET_TIMEOUT       = MT_ST_TIMEOUTS | 0x000000
MT_ST_SET_LONG_TIMEOUT  = MT_ST_TIMEOUTS | 0x100000
MT_ST_SET_CLN           = 0x80000000
MT_ST_BUFFER_WRITES     = 0x1
MT_ST_ASYNC_WRITES      = 0x2
MT_ST_READ_AHEAD        = 0x4
MT_ST_DEBUGGING         = 0x8
MT_ST_TWO_FM            = 0x10
MT_ST_FAST_MTEOM        = 0x20
MT_ST_AUTO_LOCK         = 0x40
MT_ST_DEF_WRITES        = 0x80
MT_ST_CAN_BSR           = 0x100
MT_ST_NO_BLKLIMS        = 0x200
MT_ST_CAN_PARTITIONS    = 0x400
MT_ST_SCSI2LOGICAL      = 0x800
MT_ST_SYSV              = 0x1000
MT_ST_NOWAIT            = 0x2000
MT_ST_SILI              = 0x4000
# The mode parameters to be controlled. Parameter chosen with bits 20-28 */
MT_ST_CLEAR_DEFAULT     = 0xfffff
MT_ST_DEF_DENSITY       = MT_ST_DEF_OPTIONS | 0x100000
MT_ST_DEF_COMPRESSION   = MT_ST_DEF_OPTIONS | 0x200000
MT_ST_DEF_DRVBUFFER     = MT_ST_DEF_OPTIONS | 0x300000
# The offset for the arguments for the special HP changer load command. */
MT_ST_HPLOADER_OFFSET   = 10000

'''
 * The following is for compatibility across the various Linux
 * platforms.  The generic ioctl numbering scheme doesn't really enforce
 * a type field.  De facto, however, the top 8 bits of the lower 16
 * bits are indeed used as a type field, so we might just as well make
 * this explicit here.  Please be sure to use the decoding macros
 * below from now on.
 '''
_IOC_NRBITS     = 8
_IOC_TYPEBITS   = 8

'''
 * Let any architecture override either of the following before
 * including this file.
'''
_IOC_SIZEBITS   = 14
_IOC_DIRBITS    =  2

_IOC_NRMASK     = (1 << _IOC_NRBITS)-1
_IOC_TYPEMASK   = (1 << _IOC_TYPEBITS)-1
_IOC_SIZEMASK   = (1 << _IOC_SIZEBITS)-1
_IOC_DIRMASK    = (1 << _IOC_DIRBITS)-1

_IOC_NRSHIFT    = 0
_IOC_TYPESHIFT  = (_IOC_NRSHIFT+_IOC_NRBITS)
_IOC_SIZESHIFT  = (_IOC_TYPESHIFT+_IOC_TYPEBITS)
_IOC_DIRSHIFT   = (_IOC_SIZESHIFT+_IOC_SIZEBITS)

'''
 * Direction bits, which any architecture can choose to override
 * before including this file.
'''
_IOC_NONE   = 0
_IOC_WRITE  = 1
_IOC_READ   = 2

def _IOC(tdir,ttype,nr,size):
    return(tdir << _IOC_DIRSHIFT | ord(ttype) << _IOC_TYPESHIFT |
             nr << _IOC_NRSHIFT  | size  << _IOC_SIZESHIFT)

# used to create numbers
_IO         = lambda ttype,nr:      _IOC(_IOC_NONE,ttype,nr,0)
_IOR        = lambda ttype,nr,size: _IOC(_IOC_READ,ttype,nr,sizeof(size))
_IOW        = lambda ttype,nr,size: _IOC(_IOC_WRITE,ttype,nr,sizeof(size))
_IOWR       = lambda ttype,nr,size: _IOC(_IOC_READ|_IOC_WRITE,ttype,nr,sizeof(size))
_IOR_BAD    = lambda ttype,nr,size: _IOC(_IOC_READ,ttype,nr,sizeof(size))
_IOW_BAD    = lambda ttype,nr,size: _IOC(_IOC_WRITE,ttype,nr,sizeof(size))
_IOWR_BAD   = lambda ttype,nr,size: _IOC(_IOC_READ|_IOC_WRITE,ttype,nr,sizeof(size))

# used to decode ioctl numbers..
_IOC_DIR        = lambda nr: (nr >> _IOC_DIRSHIFT) & _IOC_DIRMASK
_IOC_TYPE       = lambda nr: (nr >> _IOC_TYPESHIFT) & _IOC_TYPEMASK
_IOC_NR         = lambda nr: (nr >> _IOC_NRSHIFT) & _IOC_NRMASK
_IOC_SIZE       = lambda nr: (nr >> _IOC_SIZESHIFT) & _IOC_SIZEMASK
_IOC_SOFTERR    = lambda nr: (nr >> MT_ST_SOFTERR_SHIFT) & MT_ST_SOFTERR_MASK
_IOC_BLKSIZE    = lambda nr: (nr >> MT_ST_BLKSIZE_SHIFT) & MT_ST_BLKSIZE_MASK


# ...and for the drivers/sound files...

IOC_IN          = _IOC_WRITE << _IOC_DIRSHIFT
IOC_OUT         = _IOC_READ << _IOC_DIRSHIFT
IOC_INOUT       = (_IOC_WRITE|_IOC_READ) << _IOC_DIRSHIFT
IOCSIZE_MASK    = _IOC_SIZEMASK << _IOC_SIZESHIFT
IOCSIZE_SHIFT   = _IOC_SIZESHIFT

# mag tape io control commands */
mt_opcodes = {
    const.NDMP_MTIO_FSF: MTFSF,
    const.NDMP_MTIO_BSF: MTBSF,
    const.NDMP_MTIO_FSR: MTFSR,
    const.NDMP_MTIO_BSR: MTBSR,
    const.NDMP_MTIO_REW: MTREW,
    const.NDMP_MTIO_EOF: MTWEOF,
    const.NDMP_MTIO_OFF: MTOFFL
    } 
# TODO:  MT_TUR
