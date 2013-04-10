''' SCSI Interface 
                  This interface passes SCSI CDBs through to a SCSI device and 
                  retrieve the resulting SCSI status. The DMA uses the SCSI 
                  Interface to control locally attached tape library media changer. 
                  Software on the DMA will construct SCSI CDBs and interprets the 
                  returned status and data. The SCSI Interface MAY also exploit 
                  special features of SCSI backup devices. 
'''
import os, traceback
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config 
import xdr.ndmp_const as const
import tools.utils as ut
from tools.device import Device
from tools import cdb
from ctypes import string_at

class open():
    '''Opens the specified SCSI device. This operation is REQUIRED before
               any other SCSI requests may be executed.'''
    def request_v4(self, record):
        if (ut.check_device_not_opened(record)): return
        record.device = Device(path=record.b.device.decode())
        record.device.open(record)
            
    def reply_v4(self, record):
        pass

    request_v3 = request_v4
    reply_v3 = reply_v4
    
class close():
    '''This request closes the currently open SCSI device. No further
               requests SHALL be made until another open request is successfully
               executed.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        pass
    
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): return
        record.device.close(record)
        record.device = None

    reply_v3 = reply_v4
        
class get_state():
    '''This request returns the current state of the SCSI Interface. The
               target information provides information about which SCSI device is
               controlled by this interface.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        pass
    
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): return
        if not (c.system == 'Linux'):
            record.error = const.NDMP_NOT_SUPPORTED_ERR
        else:
            try:
                hctl = ut.add_hctl_linux(record)
            except:
                record.error = const.NDMP_NOT_SUPPORTED_ERR
            record.b.target_controller = hctl[0]
            record.b.target_id = hctl[1]
            record.b.target_lun = hctl[2]
                
    request_v3 = request_v4
    reply_v3 = reply_v4

class reset_device():
    '''This request sends a SCSI device reset message to the currently
               opened SCSI device.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        pass
    
    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): return
        record.error =  const.NDMP_NOT_SUPPORTED_ERR
        
    request_v3 = request_v4
    reply_v3 = reply_v4
    
class execute_cdb():
    '''This request sends a SCSI Control Data Block to a SCSI device. If a
        check condition is generated, then the extended sense data is also
        retrieved.'''
    def request_v4(self, record):
        if not(ut.check_device_opened(record)): return
        if not (c.system in c.Unix):
            record.error = const.NDMP_NOT_SUPPORTED_ERR
        else:
            cdb.getcdb(record)

    def reply_v4(self, record):
        if not(ut.check_device_opened(record)): return
        record.b.status = record.device.cdb.status
        record.b.dataout_len = record.device.cdb.sb_len_wr
        record.b.datain = bytes(string_at(record.device.cdb.dxferp, 
                                          min(record.device.cdb.dxfer_len, record.device.datain_len)))
        record.b.ext_sense = bytes(string_at(record.device.cdb.sbp, record.device.cdb.sb_len_wr))

    request_v3 = request_v4
    reply_v3 = reply_v4
            