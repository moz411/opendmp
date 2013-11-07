'''
This module send to devices and read from them the SCSI Control Descriptor
Blocks

From Linux kernel  /usr/include/scsi/sg.h

See http://tldp.org/HOWTO/SCSI-Generic-HOWTO/sg_io_hdr_t.html
'''
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import xdr.ndmp_const as const
from ctypes import *
from fcntl import ioctl
from os import errno


SG_DXFER_NONE = -1
SG_DXFER_TO_DEV = -2
SG_DXFER_FROM_DEV = -3
SG_DXFER_TO_FROM_DEV = -4
SG_FLAG_DIRECT_IO = 1
SG_FLAG_LUN_INHIBIT = 2
SG_FLAG_NO_DXFER = 0x10000
SENSE_SIZE = 0xFF
SG_IO = 0x2285

class SGIO(Structure):
    _fields_ = [
        ("interface_id", c_int), # [i] 'S' for SCSI generic (required)
        ("dxfer_direction", c_int), # [i] data transfer direction
        ("cmd_len", c_ubyte), # [i] SCSI command length ( <= 16 bytes)
        ("mx_sb_len", c_ubyte), # [i] max length to write to sbp
        ("iovec_count", c_ushort), # [i] 0 implies no scatter gather
        ("dxfer_len", c_uint), # [i] byte count of data transfer
        ("dxferp", c_void_p), # [i], [*io] points to data transfer memory or scatter gather list
        ("cmdp", c_void_p), # [i], [*i] points to command to perform
        ("sbp", c_void_p), # [i], [*o] points to sense_buffer memory
        ("timeout", c_uint), # [i] MAX_UINT->no timeout (unit: millisec)
        ("flags", c_uint), # [i] 0 -> default, see SG_FLAG...
        ("pack_id", c_int), # [i->o] unused internally (normally)
        ("usr_ptr", c_void_p), # [i->o] unused internally
        ("status", c_ubyte), # [o] scsi status
        ("masked_status", c_ubyte), # [o] shifted, masked scsi status
        ("msg_status", c_ubyte), # [o] messaging level data (optional)
        ("sb_len_wr", c_ubyte), # [o] byte count actually written to sbp
        ("host_status", c_ushort), # [o] errors from host adapter
        ("driver_status", c_ushort), # [o] errors from software driver
        ("resid", c_int), # [o] dxfer_len - actual_transferred
        ("duration", c_uint), # [o] time taken by cmd (unit: millisec)
        ("info", c_uint) # [o] auxiliary information
        ]
    
    def __repr__(self):
        return ("SGIO(interface_id={self.interface_id}, dxfer_direction={self.dxfer_direction}, " +
                "cmd_len={self.cmd_len}, mx_sb_len={self.mx_sb_len}, iovec_count={self.iovec_count}, " +
                "dxfer_len={self.dxfer_len}, dxferp={self.dxferp}, cmdp={self.cmdp}, sbp={self.sbp}, " +
                "timeout={self.timeout}, flags={self.flags}, pack_id={self.pack_id}, usr_ptr={self.usr_ptr}, " +
                "status={self.status}, masked_status={self.masked_status}, msg_status={self.msg_status}, " +
                "sb_len_wr={self.sb_len_wr}, host_status={self.host_status}, driver_status={self.driver_status}, " +
                "resid={self.resid}, duration={self.duration}, info={self.info})").format(self=self)



def getcdb(record):
    record.device.datain_len = record.b.datain_len
    buf = create_string_buffer(sizeof(SGIO))
    memset(buf, 0, sizeof(SGIO))
    sgio = SGIO.from_buffer(buf)
    sgio.interface_id = ord('S')
    sgio.pack_id = 0
    sgio.timeout = record.b.timeout
    sgio.flags = SG_FLAG_DIRECT_IO
    
    sgio.command_buffer = create_string_buffer(record.b.cdb, len(record.b.cdb))
    sgio.cmd_len = sizeof(sgio.command_buffer)
    sgio.cmdp = cast(sgio.command_buffer, c_void_p)
    
    sgio.data_buffer = create_string_buffer(record.b.datain_len)
    sgio.dxfer_len = sizeof(sgio.data_buffer)
    sgio.dxferp = cast(sgio.data_buffer, c_void_p)
    sgio.dxfer_direction = SG_DXFER_FROM_DEV
    
    sgio.sense_buffer = create_string_buffer(SENSE_SIZE)
    sgio.mx_sb_len = sizeof(sgio.sense_buffer)
    sgio.sbp = cast(sgio.sense_buffer, c_void_p)
    
    try:
        # send CDB
        ioctl(record.device.fd, SG_IO, sgio)
    except (OSError, IOError) as e :
        stdlog.error(record.device.path + ': ' + e.strerror)
        record.error = const.NDMP_IO_ERR
        if(e.errno == errno.EACCES):
            record.error = const.NDMP_WRITE_PROTECT_ERR
        if(e.errno == errno.ENOENT):
            record.error = const.NDMP_NO_DEVICE_ERR
        if(e.errno == errno.EBUSY):
            record.error = const.NDMP_DEVICE_BUSY_ERR
        if(e.errno == errno.ENODEV):
            record.error = const.NDMP_NO_DEVICE_ERR
        if(e.errno == 46): # ENOTREADY
            record.error = const.NDMP_NO_TAPE_LOADED_ERR
    except:
        stdlog.error(record.device.path + ': ioctl failed')
        record.error = const.NDMP_IO_ERR
        stdlog.debug(repr(sgio))
        
    record.device.cdb = sgio
