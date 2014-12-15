'''This module gives the backup commands used for "tar" Backup Unit
TODO: implement a real plugin system'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from tools import utils as ut
from xdr import ndmp_const as const
from xdr.ndmp_type import ndmp_butype_info, ndmp_pval
from server.bu import Backup_Utility
import re, asyncio

class Star(): 
    
    name = 'star'
    ostype = c.Unix
    executable = '/usr/bin/star'
    butype_info = ndmp_butype_info(
                    butype_name = b'tar',
                    default_env = [ndmp_pval(name=b'USER', value=cfg['USER'].encode()),
                                ndmp_pval(name=b'PATHNAME_SEPARATOR', value=b'/')],
                    attrs = (
                          const.NDMP_BUTYPE_BACKUP_FILELIST |
                          const.NDMP_BUTYPE_RECOVER_FILELIST |
                          const.NDMP_BUTYPE_BACKUP_INCREMENTAL |
                          const.NDMP_BUTYPE_RECOVER_INCREMENTAL |
                          const.NDMP_BUTYPE_BACKUP_UTF8 |
                          const.NDMP_BUTYPE_RECOVER_UTF8 |
                          const.NDMP_BUTYPE_BACKUP_FH_FILE |
                          const.NDMP_BUTYPE_RECOVER_FH_FILE |
                          const.NDMP_BUTYPE_BACKUP_FH_DIR |
                          const.NDMP_BUTYPE_RECOVER_FH_DIR
                          )
                    )
    
    @asyncio.coroutine
    def backup(self):
        # Preparing command line
        if (c.system == 'FreeBSD'):
            command_line = '-c -dump -no-fifo f=FIFO -find . INCREMENTAL -xdev -exec stat -r {} \;'
        elif (c.system == 'Linux'):
            command_line = '-c -dump -no-fifo f=FIFO -find . INCREMENTAL -xdev -exec stat -c "%d %i %f %h %u %g %d %s %X %Y %Z %Z %o %b %Z %n" {} \;'
        
        command_line = re.sub('FIFO', self.fifo, command_line)
        command_line = re.sub('FILESYSTEM', self.env['FILESYSTEM'], command_line)
        
        # Is it an incremental backup?
        try:
            self.env['LEVEL'] = int(self.env['LEVEL'])
        except (KeyError, ValueError):
            stdlog.error('Cannot decode level, using 0')
            self.env['LEVEL'] = 0
            command_line = re.sub('INCREMENTAL', '', command_line)
        else:
            dumpdates = ut.read_dumpdates(cfg['DUMPDATES'])
            tstamp = ut.compute_incremental(dumpdates, 
                                            self.env['FILESYSTEM'],
                                            self.env['LEVEL'])
            self.record.data['server'].stampfile = ut.mktstampfile(self.record.data['server'].tstamp, tstamp)
            command_line = re.sub('INCREMENTAL', '-newer ' + 
                                  self.record.data['server'].tstamp, command_line)
        
        stdlog.info('Backup level ' + repr(self.env['LEVEL']))
        
        # Launch the backup process
        loop = asyncio.get_event_loop()
        self.process,  =  loop.subprocess_exec(Backup_Utility,
                                            self.executable,'-vvv',command_line,
                                            cwd=self.env['FILESYSTEM'],
                                            stdout=asyncio.subprocess.PIPE,
                                            stderr=asyncio.subprocess.PIPE)
            
        
    def recover(self):
        filesystem = ''
        rename = '-s /OLD/NEW/'
        
        '''original_path
        The original path name of the data to be recovered,
        relative to the backup root. If original_path is the null
        string, the server shall recover all data contained in the
        backup image.'''
        if not(self.record.data['nlist']['original_path'] == ''):
            original_path = self.record.data['nlist']['original_path']
            filesystem = self.record.data['nlist']['original_path']
        if self.record.data['nlist']['original_path'] == '/':
            filesystem = ''
        

        '''If name is the null string:
        destination_path identifies the name to which the data
        identified by original_path are to be recovered.
        other_name must be the null string.'''
        if(self.record.data['nlist']['new_name'] != ''):
            rename = re.sub('OLD', original_path, rename)
            rename = re.sub('NEW', self.record.data['nlist']['new_name'], rename)
            
        '''If other_name is not the null string:
        destination_path, when concatenated with the server-
        specific path name delimiter and other_name,
        identifies the alternate name-space name of the data
        to be recovered. The definition of such alternate
        name-space is server-specific.'''
        if(self.record.data['nlist']['other_name'] != ''):
            rename = re.sub('OLD', original_path, rename)
            rename = re.sub('NEW', self.record.data['nlist']['other_name'], rename)
        
        
        # Preparing command line
        command_line = 'star -x -B -U RENAME -no-fifo f=UNIXSOCKET FILESYSTEM'
        command_line = re.sub('UNIXSOCKET', self.fifo, command_line)
        command_line = re.sub('FILESYSTEM', filesystem, command_line)
        if(rename == '-s /OLD/NEW/'):
            rename = ''
        command_line = re.sub('RENAME', rename, command_line)
            
        
        return (command_line)
