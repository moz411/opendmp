''' The NDMP Server uses this interface to send file history entries to
the DMA. The file history entries provide a file by file record of
every file backed up by the backup method. The file history data is
defined using a UNIX file system or an NT file system compatible
format. The backup method can generate UNIX, NT, or both UNIX and NT
file system compatible format file history for each file.'''

from tools.log import Log; stdlog = Log.stdlog
from tools.config import Config; cfg = Config.cfg; c = Config
from xdr import ndmp_const as const
import tools.utils as ut

class add_file():
    '''This request adds a list of file paths with the corresponding
        attribute entries to the file history'''

    @ut.post('ndmp_fh_add_file_request_v3',const.NDMP_FH_ADD_FILE)
    async def post(self, record):
        record.post_body.files = record.bu['bu'].history

class add_dir():
    '''This message is used to report name and inode information for backed
        up files'''
    
    async def post(self, record):
        pass

class add_node():
    '''This request adds a list of file attribute entries to the file
        history'''
    
    async def post(self, record):
        pass