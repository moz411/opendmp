'''This module gives the backup commands used for "tar" Backup Unit
TODO: implement a real plugin system'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from xdr import ndmp_type as types
from server.bu import Backup_Utility
from tools import utils as ut
import time,re,pwd,grp

class Tar(Backup_Utility): 
    
    name = 'tar'
    ostype = c.Unix
    executable = '/bin/tar'
    args = '-cPvv -H pax -f FIFO FILESYSTEM'
    butype_info = types.ndmp_butype_info(
                    butype_name = b'tar',
                    default_env = [types.ndmp_pval(name=b'USER', value=cfg['USER'].encode()),
                                types.ndmp_pval(name=b'PATHNAME_SEPARATOR', value=b'/')],
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
    
    def add_file(self, line):
        '''drwxr-xr-x root/root         0 2016-04-04 19:14 /opt/VBoxGuestAdditions-5.0.4/'''
        (fattr,owner_group,size,ctime1,ctime2,file) = line.split()
        file_stat = types.ndmp_file_stat_v3(
                            invalid=const.NDMP_FILE_STAT_ATIME_INVALID|const.NDMP_FILE_STAT_CTIME_INVALID,
                            fs_type=const.NDMP_FS_UNIX,
                            ftype=ut.check_mode_file(fattr[0]),
                            mtime=int(time.mktime(time.strptime(ctime1+' '+ctime2, '%Y-%m-%d %H:%M'))),
                            atime=0,
                            ctime=0,
                            fattr=int(0o0700),
                            owner=int(pwd.getpwnam(owner_group.split('/')[0]).pw_uid),
                            group=int(grp.getgrnam(owner_group.split('/')[1]).gr_gid),
                            size=ut.long_long_to_quad(int(size)),
                            links=0)
        node = ut.long_long_to_quad(0)
        file_name = types.ndmp_file_name_v3(const.NDMP_FS_UNIX, repr(file).encode(encoding='utf_8', errors='strict'))
        fh_info = ut.long_long_to_quad(1>>64)
        return types.ndmp_file([file_name],[file_stat], node, fh_info)
    