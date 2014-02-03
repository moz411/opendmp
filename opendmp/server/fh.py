'''This module process the file history stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import asyncore, traceback
from interfaces import fh
from tools import utils as ut

class Fh(asyncore.file_dispatcher):
    
    def __init__(self, record):
        self.record = record
        self.file = open(self.record.fh['history'], mode='rb')
        asyncore.file_dispatcher.__init__(self, self.file)
        stdlog.info('[%d] Starting File History of ' + self.record.data['env']['FILESYSTEM'],
                    record.fileno)
        
    def writable(self):
        return False
        
    def readeable(self):
        return True

    def handle_read(self):
        line = self.file.readline()
        if line: self.record.fh['files'].append(line.strip())
        else: self.handle_close()
        if len(self.record.fh['files']) >= self.record.fh['max_lines']:
                        fh.add_file().post(self.record)

    def handle_error(self):
        stdlog.info('FH> Listing read failed')
        stdlog.debug(traceback.print_exc())
        self.handle_close()
        
    def handle_close(self):
        if (len(self.record.fh['files']) > 0):
                fh.add_file().post(self.record)
        self.close()
        ut.clean_file(self.record.fh['history'])
        stdlog.info('[%d] File History operation finished', self.record.fileno)

    def log(self, message):
        stdlog.debug('[%d] Fh ' + message, self.record.fileno)

    def log_info(self, message, type='info'):
        stdlog.info('[%d] Fh ' + message, self.record.fileno)