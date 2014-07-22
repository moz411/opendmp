''' The NDMP Server uses this interface to send file history entries to
the DMA. The file history entries provide a file by file record of
every file backed up by the backup method. The file history data is
defined using a UNIX file system or an NT file system compatible
format. The backup method can generate UNIX, NT, or both UNIX and NT
file system compatible format file history for each file.'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
import re, traceback
from xdr import ndmp_const as const, ndmp_type as types
import tools.utils as ut

class add_file():
    '''This request adds a list of file paths with the corresponding
        attribute entries to the file history'''
    
    @ut.post('ndmp_fh_add_file_request_v3',const.NDMP_FH_ADD_FILE)
    def post(self, record):
        record.post_body.files = []
        mode = re.compile('(\d{,3})(\d{4})')
        sep = record.data['bu'].env['PATHNAME_SEPARATOR']
        #st_dev st_ino st_mode st_nlink st_uid st_gid st_rdev st_size st_atime st_mtime st_ctime 
        # st_birthtime st_blksize st_blocks st_gen name 
        
        for line in record.data['fh'].history:
            try:
                (st_dev, st_ino, st_mode, st_nlink, st_uid, st_gid, st_rdev, 
                 st_size, st_atime, st_mtime, st_ctime, st_birthtime, 
                 st_blksize, st_blocks, st_gen, name) = line.split(None, 15) 
                if(c.system == 'Linux'):
                    st_mode = oct(int(bytes(st_mode,('utf-8')).decode(),base=16)).split('o')[1]
                res = mode.match(st_mode)
                ftype = ut.check_file_mode(int(res.group(1)))
                fattr = res.group(2)
            except:
                print(line)
                traceback.print_exc()
            
            if(c.system in c.Unix):
                try:
                    file_stat = types.ndmp_file_stat_v3(invalid=0,
                                                   fs_type=const.NDMP_FS_UNIX,
                                                   ftype=ftype,
                                                   atime=int(st_atime),
                                                   mtime=int(st_mtime),
                                                   ctime=int(st_ctime),
                                                   fattr=int(fattr),
                                                   owner=int(st_uid),
                                                   group=int(st_gid),
                                                   size=ut.long_long_to_quad(int(st_size)),
                                                   links=int(st_nlink))
                    node = ut.long_long_to_quad(int(st_ino))
                    
                    if(name != '.'):
                        if (name.startswith('./')):
                            name = re.sub('^./','',name)
                        name = ''.join([sep,name])
                        file_name = types.ndmp_file_name_v3(const.NDMP_FS_UNIX, 
                                            repr(name).encode(encoding='utf_8', errors='strict'))
                        fh_info = ut.long_long_to_quad(1>>64)
                        record.post_body.files.append(types.ndmp_file([file_name], 
                                                    [file_stat], node, fh_info))
                except:
                    print(line)
                    traceback.print_exc()


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