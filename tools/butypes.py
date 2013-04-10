'''This configuration file defines the different backup and restore tools available for 
every platform'''

from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
from xdr.ndmp_type import ndmp_butype_info, ndmp_pval

Unix = ['tar']
Windows = ['wbadmin', 'ntbackup']

tar = ndmp_butype_info(butype_name=b'tar',
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

tar.options = {'backup': '-cP',
               'recover': '-x -C'}

wbadmin = ndmp_butype_info(butype_name=b'wbadmin',
                       default_env=[ndmp_pval(name=b'USER', value=cfg['USER'].encode())],
                       attrs= (
                              const.NDMP_BUTYPE_BACKUP_FILELIST |
                              const.NDMP_BUTYPE_RECOVER_FILELIST |
                              const.NDMP_BUTYPE_BACKUP_INCREMENTAL |
                              const.NDMP_BUTYPE_RECOVER_INCREMENTAL |
                              const.NDMP_BUTYPE_BACKUP_UTF8 |
                              const.NDMP_BUTYPE_RECOVER_UTF8
                              )
            )


ntbackup = ndmp_butype_info(butype_name=b'ntbackup',
                       default_env=[ndmp_pval(name=b'USER', value=cfg['USER'].encode())],
                       attrs= (
                              const.NDMP_BUTYPE_BACKUP_FILELIST |
                              const.NDMP_BUTYPE_RECOVER_FILELIST |
                              const.NDMP_BUTYPE_BACKUP_INCREMENTAL |
                              const.NDMP_BUTYPE_RECOVER_INCREMENTAL |
                              const.NDMP_BUTYPE_BACKUP_UTF8 |
                              const.NDMP_BUTYPE_RECOVER_UTF8 
                              )
            )
        