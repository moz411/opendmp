'''This module create a asyncore Consumer for Data operations 
and process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import asyncore, time
from xdr import ndmp_const as const
from interfaces import notify as nt
from tools import utils as ut
from subprocess import TimeoutExpired


class Data(asyncore.dispatcher):
    
    def __init__(self, record):
        asyncore.dispatcher.__init__(self, record.data['peer'])
        self.record = record
        self.errmsg = None
        if record.data['operation'] == const.NDMP_DATA_OP_BACKUP:
            stdlog.info('Starting backup of ' + self.record.data['env']['FILESYSTEM'])
            self.file = open(self.record.data['bu_fifo'],'rb')
        else:
            stdlog.info('Starting recover of ' + self.record.data['env']['FILESYSTEM'])
            self.file = open(self.record.data['bu_fifo'],'wb')
            nt.data_read().post(self.record)
    
    def writeable(self): # Backup
        if (self.record.data['operation'] == const.NDMP_DATA_OP_BACKUP
            and self.record.data['state'] == const.NDMP_DATA_STATE_ACTIVE):
            return True
        
    def readeable(self): # Recover
        if (self.record.data['operation'] == const.NDMP_DATA_OP_RECOVER
            and self.record.data['state'] == const.NDMP_DATA_STATE_ACTIVE):
            return True

    def handle_write(self): # Backup
        data = self.file.read(int(cfg['BUFSIZE']))
        self.record.data['stats']['current'] += self.send(data)

    def handle_read(self): # Recover
        data = self.recv(int(cfg['BUFSIZE']))
        self.record.data['stats']['current'] += self.file.write(data)

    def handle_error(self):
        self.record.error = const.NDMP_ILLEGAL_STATE_ERR
        self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
        stdlog.error('DATA> Operation failed')
        
    def handle_close(self):
        try:
            self.record.data['process'].wait(5)
        except TimeoutExpired:
            stdlog.error('killing bu process')
            self.record.data['process'].kill()
            self.record.data['process'].wait()
        try:
            self.record.data['process'].poll()
            if self.errmsg:
                self.record.data['retcode'] = 255
                self.record.data['error'].append(self.errmsg)
            else:    
                self.record.data['retcode'] = self.record.data['process'].returncode
            with open(self.record.data['bu_fifo'] + '.err', 'rb') as logfile:
                for line in logfile:
                    self.record.data['error'].append(line.strip())
            self.close()
            # cleanup temporary files
            for tmpfile in [self.record.data['bu_fifo'] + '.err', 
                            self.record.data['stampfile'],
                            self.record.data['bu_fifo']]:
                if tmpfile is not None: ut.clean_file(tmpfile)
        except (OSError, ValueError, AttributeError) as e:
            stdlog.error('DATA> close operation failed:' + repr(e))
            
        self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
        self.record.data['operation'] = const.NDMP_DATA_OP_NOACTION
        if (self.record.data['retcode'] == 0):
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
        else:
            self.record.error = const.NDMP_ILLEGAL_STATE_ERR
            self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
        stdlog.info('DATA> BU finished status ' + repr(self.record.data['retcode']))
        
    def pause(self):
        pass
            
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.record.data['dumpdates'].update({(self.record.data['env']['FILESYSTEM'],
                                                   self.record.data['env']['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))
