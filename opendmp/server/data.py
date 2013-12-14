'''This module process the data stream'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import socket, threading, sys, time, traceback
from xdr import ndmp_const as const
from interfaces import notify as nt
from tools import utils as ut
from subprocess import TimeoutExpired

class Data(threading.Thread):
    
    def __init__(self, record):
        threading.Thread.__init__(self, name='Data-' + repr(threading.activeCount()))
        self.record = record
        self.errmsg = None
        
    def run(self):
        try:
            if self.record.data['operation'] == const.NDMP_DATA_OP_BACKUP:
                stdlog.info('Starting backup of ' + self.record.data['env']['FILESYSTEM'])
                self.backup()
                self.terminate()
                self.record.fh['barrier'].wait() # wait for File History to send all logs
                if self.record.data['retcode'] == 0:
                    self.update_dumpdate()
            elif self.record.data['operation'] == const.NDMP_DATA_OP_RECOVER:
                stdlog.info('Starting recover of ' + self.record.data['env']['FILESYSTEM'])
                try:
                    self.recover()
                except socket.timeout:
                    stdlog.error('No more data')
                self.terminate()
        except Exception as e:
            self.errmsg = repr(e)
            stdlog.error('operation failed: ' + self.errmsg)
            self.terminate()
        finally:
            nt.data_halted().post(self.record)
            stdlog.info('Data operation finished status ' + repr(self.record.data['retcode']))
            sys.exit()

    def backup(self):
        with open(self.record.data['bu_fifo'],'rb') as file:
            while not self.record.data['equit'].is_set():
                data = file.read(int(cfg['BUFSIZE']))
                with self.record.data['lock']:
                    self.record.data['stats']['current'] += self.record.data['fd'].send(data)
                if not data: return

    def recover(self):
        # For NetWorker, does not seems to close Tape server socket
        self.record.data['fd'].settimeout(3)
        with open(self.record.data['bu_fifo'],'wb') as file:
            nt.data_read().post(self.record)
            while not self.record.data['equit'].is_set():
                data = self.record.data['fd'].recv(int(cfg['BUFSIZE']))
                with self.record.data['lock']:
                    self.record.data['stats']['current'] += file.write(data)
                if not data: return
            
    def terminate(self):
        try:
            self.record.data['process'].wait(5)
        except TimeoutExpired:
            stdlog.error('killing bu process')
            self.record.data['process'].kill()
            
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
            self.record.data['fd'].close()
            # cleanup temporary files
            for tmpfile in [self.record.data['bu_fifo'] + '.err', 
                            self.record.data['stampfile'],
                            self.record.data['bu_fifo']]:
                if tmpfile is not None: ut.clean_file(tmpfile)
        except (OSError, ValueError, AttributeError) as e:
            stdlog.error('terminate operation failed:' + repr(e))
            
        with self.record.data['lock']:
            self.record.data['state'] = const.NDMP_DATA_STATE_HALTED
            self.record.data['operation'] = const.NDMP_DATA_OP_NOACTION
            if (self.record.data['retcode'] == 0):
                self.record.data['halt_reason'] = const.NDMP_DATA_HALT_SUCCESSFUL
            else:
                self.record.error = const.NDMP_ILLEGAL_STATE_ERR
                self.record.data['halt_reason'] = const.NDMP_DATA_HALT_INTERNAL_ERROR
            
            
    def update_dumpdate(self):            
        try:
            # Update dumpdates
            self.record.data['dumpdates'].update({(self.record.data['env']['FILESYSTEM'],
                                                   self.record.data['env']['LEVEL']):int(time.time())})
            ut.write_dumpdates('.'.join([cfg['DUMPDATES'], self.record.data['bu_type']]),
                               self.record.data['dumpdates'])
        except (OSError, ValueError, UnboundLocalError) as e:
            stdlog.error('update dumpdate failed' + repr(e))
            
            
class Wait_Connection(threading.Thread):
    
    def __init__(self, record):
        threading.Thread.__init__(self, name='Wait_Connection-' + repr(threading.activeCount()))
        self.record = record
    
    def run(self):
        try:
            with self.record.data['lock']:
                s = self.record.data['local_address']
            stdlog.info('Data Listening on port ' + repr(s.getsockname()))
            client, address = s.accept()
            with self.record.data['lock']:
                self.record.data['fd'] = client
                self.record.data['state'] = const.NDMP_DATA_STATE_CONNECTED
            stdlog.info('Data connection from ' + repr(address))
        except:
            traceback.print_exc()
        finally:
            sys.exit()
