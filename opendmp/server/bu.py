from tools import utils as ut
from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from interfaces import notify as nt, log as ndmplog, fh
import asyncore, os, queue, traceback
from subprocess import TimeoutExpired

class Backup_Utility(asyncore.file_dispatcher):
        
    def __init__(self, record):
        self.record = record
        self.env = {}
        self.queue = queue.Queue(maxsize=100*(int(cfg['BUFSIZE'])))
        # FIFO for data transfer with Backup Utility
        self.fifo = ut.give_fifo()
        # File History
        self.history = self.fifo + '.lst'
        # BU error log
        self.errorlog = self.fifo + '.err'
        # subprocess
        self.process = None
        # subprocess return code
        self.retcode = 0
        self.errmsg = []
        
        file = os.open(self.fifo, os.O_RDWR | os.O_NONBLOCK)
        asyncore.file_dispatcher.__init__(self, file)
        os.close(file)
          
    def readable(self): # Backup
        return True if (self.record.data['operation'] == const.NDMP_DATA_OP_BACKUP and
            self.record.data['state'] == const.NDMP_DATA_STATE_ACTIVE and 
            self.record.error == const.NDMP_NO_ERR and 
            not self.queue.full()) else False
    
    def writable(self): # Recover
        return True if (self.record.data['operation'] == const.NDMP_DATA_OP_RECOVER and
            self.record.data['state'] == const.NDMP_DATA_STATE_ACTIVE and
            self.record.error == const.NDMP_NO_ERR) else False

    def handle_read(self): # Backup
        data = self.read(int(cfg['BUFSIZE']))
        if data == b'\x00'*int(cfg['BUFSIZE']): # maybe the subprocess have finished
            try:
                self.process.wait(.1)
            except TimeoutExpired: # subprocess not finished
                self.queue.put(data)
            else:
                self.queue.put(None) # Poison pill for Data
                self.handle_close()
        self.queue.put(data)

    def handle_write(self): # Recover
        self.write(self.queue.get())

    def handle_error(self):
        traceback.print_exc()
        self.handle_close()
        
    @ut.async_opened
    def handle_close(self):
        # Close current fifo
        self.close()
        
        # Kill BU process
        try:
            self.process.wait(5)
        except TimeoutExpired:
            stdlog.error('killing bu process')
            self.process.kill()
            self.process.wait()
        try:
            self.process.poll()
            self.retcode = self.process.returncode
        except (OSError, ValueError, AttributeError) as e:
            stdlog.error('BU close operation failed:' + repr(e))
            self.retcode = 255
        
        # retrieve bu log file
        with open(self.errorlog, 'rb') as logfile:
            for line in logfile:
                self.errmsg.append(line.strip())
        ndmplog.message().post(self.record)
        
        # Cleanup temp files
        for file in [self.fifo, self.history, self.errorlog]:
            ut.clean_file(file)
            
    def log(self, message):
        stdlog.debug(message)

    def log_info(self, message, type='info'):
        stdlog.info(message)