'''This module gives the backup commands used for "tar" Backup Unit
TODO: implement a real plugin system'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from tools import utils as ut
from xdr import ndmp_const as const
from xdr.ndmp_type import ndmp_butype_info, ndmp_pval
import re, traceback, os

info = ndmp_butype_info(butype_name=b'tar',
                       default_env=[ndmp_pval(name=b'USER', value=cfg['USER'].encode()),
                                    ndmp_pval(name=b'PATHNAME_SEPARATOR', value=b'/')],
                       attrs= (
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

class Bu():
    
    def backup(self, record):
        
        # Retrieving FILESYSTEM to backup
        # Retrieving FILESYSTEM to backup
        try:
            if(record.data['env']['FILES']):
                record.data['env']['FILESYSTEM'] = record.data['env']['FILES']
        except KeyError:
            pass
        try:
            assert(record.data['env']['FILESYSTEM'] != None)
        except (KeyError, AssertionError) as e:
            stdlog.error('variable FILESYSTEM does not exists')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            raise
        
        # Preparing FIFO for interaction with Backup Utility
        record.data['bu_fifo'] = ut.give_fifo()
        #record.data['bu_fifo'] = ut.give_socket(record.data['fd'])
        record.fh['history'] = record.data['bu_fifo'] + '.lst'
        
        # Preparing command line
        if (c.system == 'FreeBSD'):
            command_line = 'star -c -dump -no-fifo f=UNIXSOCKET -find . INCREMENTAL -xdev -exec stat -r {} \;'
        elif (c.system == 'Linux'):
            #command_line = 'star -c -dump -no-fifo f=UNIXSOCKET -find . INCREMENTAL -xdev -exec stat -c "%d %i %f %h %u %g %d %s %X %Y %Z %Z %o %b %Z %n" {} \;'
            command_line = 'star -c -xdev -sparse -acl -link-dirs level=0 -wtardumps -no-fifo f=UNIXSOCKET -C /boot .'
           
            #command_line = 'star -v -c -no-fifo f=UNIXSOCKET .'
        
        command_line = re.sub('UNIXSOCKET', record.data['bu_fifo'], command_line)
        
        # Is it an incremental backup?
        try:
            record.data['env']['LEVEL'] = int(record.data['env']['LEVEL'])
        except KeyError:
            record.data['env']['LEVEL'] = 0
            command_line = re.sub('INCREMENTAL', '', command_line)
        else: 
            record.data['dumpdates'] = ut.read_dumpdates('.'.join([cfg['DUMPDATES'],record.data['bu_type']]))
            tstamp = ut.compute_incremental(record.data['dumpdates'], record.data['env']['FILESYSTEM'],
                                            record.data['env']['LEVEL'])
            record.data['stampfile'] = ut.mktstampfile('.'.join([record.data['bu_fifo'],'tstamp']), tstamp)
            command_line = re.sub('INCREMENTAL', '-newer ' + record.data['stampfile'], command_line)
        return (command_line)
    
    def recover(self, record):
        filesystem = ''
        rename = '-s /OLD/NEW/'
        
        '''original_path
        The original path name of the data to be recovered,
        relative to the backup root. If original_path is the null
        string, the server shall recover all data contained in the
        backup image.'''
        if not(record.data['nlist']['original_path'] == ''):
            original_path = record.data['nlist']['original_path']
            filesystem = record.data['nlist']['original_path']
        if record.data['nlist']['original_path'] == '/':
            filesystem = ''
        

        '''If name is the null string:
        destination_path identifies the name to which the data
        identified by original_path are to be recovered.
        other_name must be the null string.'''
        if(record.data['nlist']['new_name'] != ''):
            rename = re.sub('OLD', original_path, rename)
            rename = re.sub('NEW', record.data['nlist']['new_name'], rename)
            
        '''If other_name is not the null string:
        destination_path, when concatenated with the server-
        specific path name delimiter and other_name,
        identifies the alternate name-space name of the data
        to be recovered. The definition of such alternate
        name-space is server-specific.'''
        if(record.data['nlist']['other_name'] != ''):
            rename = re.sub('OLD', original_path, rename)
            rename = re.sub('NEW', record.data['nlist']['other_name'], rename)
        
        # Preparing FIFO for interaction with Backup Utility
        record.data['bu_fifo'] = ut.give_fifo()
        
        # Preparing command line
        command_line = 'star -x -B -U RENAME -no-fifo f=UNIXSOCKET FILESYSTEM'
        command_line = re.sub('UNIXSOCKET', record.data['bu_fifo'], command_line)
        command_line = re.sub('FILESYSTEM', filesystem, command_line)
        if(rename == '-s /OLD/NEW/'):
            rename = ''
        command_line = re.sub('RENAME', rename, command_line)
            
        
        return (command_line)
        