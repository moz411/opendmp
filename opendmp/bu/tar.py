'''This module gives the backup commands used for "tar" Backup Unit
TODO: implement a real plugin system'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from xdr.ndmp_type import ndmp_butype_info, ndmp_pval
from server.bu import Backup_Utility
import asyncio
from shutil import make_archive
from shlex import shlex

class Tar(Backup_Utility): 
    
    name = 'tar'
    ostype = c.Unix
    executable = '/bin/tar'
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
