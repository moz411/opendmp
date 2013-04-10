''' The NDMP Server uses this interface to send file history entries to
the DMA. The file history entries provide a file by file record of
every file backed up by the backup method. The file history data is
defined using a UNIX file system or an NT file system compatible
format. The backup method can generate UNIX, NT, or both UNIX and NT
file system compatible format file history for each file.'''

from xdr import ndmp_const as const, ndmp_type as type
from server.log import Log; stdlog = Log.stdlog
from server.config import Config; cfg = Config.cfg; c = Config
import tools.utils as ut
import time, stat
from xdr.ndmp_pack import NDMPPacker

class add_file():
    '''This request adds a list of file paths with the corresponding
        attribute entries to the file history'''
    
    def post(self, record):
        p = NDMPPacker()
        # Header
        header = type.ndmp_header()
        header.message = const.NDMP_FH_ADD_FILE
        header.message_type = const.NDMP_MESSAGE_REQUEST
        header.reply_sequence = 0
        header.sequence = record.server_sequence
        record.server_sequence += 1
        header.time_stamp = int(time.time())
        header.error = const.NDMP_NO_ERR
        p.pack_ndmp_header(header)
        
        # Body
        body = type.ndmp_fh_add_file_request_v3()
        body.files = []
        for file in record.data['stats']['files']:
            stats = file[1]
            if (stat.S_ISREG(stats.st_mode)):
                ftype = const.NDMP_FILE_REG
            elif(stat.S_ISLNK(stats.st_mode)):
                ftype = const.NDMP_FILE_SLINK
            elif(stat.S_ISFIFO(stats.st_mode)):
                ftype = const.NDMP_FILE_FIFO
            elif(stat.S_ISBLK(stats.st_mode)):
                ftype = const.NDMP_FILE_BSPEC
            elif(stat.S_ISCHR(stats.st_mode)):
                ftype = const.NDMP_FILE_CSPEC
            elif(stat.S_ISSOCK(stats.st_mode)):
                ftype = const.NDMP_FILE_SOCK
            
            if(c.system in c.Unix):
                file_name = type.ndmp_file_name_v3(const.NDMP_FS_UNIX, repr(file[0]).encode())
                file_stat = type.ndmp_file_stat_v3(invalid=0,
                                               fs_type=const.NDMP_FS_UNIX,
                                               ftype=ftype,
                                               mtime=int(stats.st_mtime),
                                               atime=int(stats.st_atime),
                                               ctime=int(stats.st_ctime),
                                               owner=int(stats.st_uid),
                                               group=int(stats.st_gid),
                                               fattr=int(stats.st_mode),
                                               size=ut.long_long_to_quad(stats.st_size),
                                               links=0)
            
                node = ut.long_long_to_quad(stats.st_ino)
                fh_info = ut.long_long_to_quad(1>>64)
                body.files.append(type.ndmp_file([file_name], [file_stat], node, fh_info))
        
        p.pack_ndmp_fh_add_file_request_v4(body)
        stdlog.debug(body)
        return p.get_buffer()


class add_dir():
    '''This message is used to report name and inode information for backed
        up files'''
    
    def post(self, record):
        pass

class add_node():
    '''This request adds a list of file attribute entries to the file
        history'''
    
    def post(self, record):
        pass
