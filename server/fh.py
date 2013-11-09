'''This module process the file history stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import threading, sys, time, traceback, stat, os, select
from xdr import ndmp_const as const
from interfaces import notify as nt, fh
from tools import utils as ut

class Fh(threading.Thread):
    
    def __init__(self, record):
        threading.Thread.__init__(self, name='Fh-' + repr(threading.activeCount()))
        self.record = record
        
    def run(self):
        stdlog.info('Starting File History of ' + self.record.data['env']['FILESYSTEM'])
        with open(self.record.fh['history'], mode='rb') as file:
            while True:
                time.sleep(0.001)
                try:
                    (read, write, error) = select.select([file.fileno()], [], [])
                    if read:
                        line = file.readline()
                        if line:
                            with self.record.fh['lock']:
                                self.record.fh['files'].append(line.strip())
                        else:
                            with self.record.data['lock']:
                                if self.record.data['state'] == const.NDMP_DATA_STATE_HALTED:
                                    break
                    if len(self.record.fh['files']) >= self.record.fh['max_lines']:
                        fh.add_file().post(self.record)
                    if self.record.fh['equit'].is_set():
                        break
                except OSError as e:
                    stdlog.error(e)
                    sys.exit()
        try:
            if (len(self.record.fh['files']) > 0):
                fh.add_file().post(self.record)
            ut.clean_file(self.record.fh['history'])
        except OSError as e:
            stdlog.error(e)
        finally:
            self.record.fh['barrier'].wait() # Will wake up data thread
            stdlog.info('File History operation finished')
            sys.exit()
        
