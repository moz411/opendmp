'''This module process the file history stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import asyncio, traceback
from interfaces import fh
from tools import utils as ut

class Fh():
    
    def __init__(self, record):
        self.record = record
        self.file = self.record.data['bu'].process.stdout
        self.loop = asyncio.get_event_loop()
        
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
        self.loop.remove_reader(self.file)
        self.file.close()
        ut.clean_file(self.record.data['bu'].history)
        stdlog.info('File History operation finished')

    def start(self):
        # Register the file descriptor for read event
        stdlog.info('Starting File History of ' + self.record.data['bu'].env['FILESYSTEM'])
        self.loop.add_reader(self.file, self.handle_read)