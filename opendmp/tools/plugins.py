''' from http://onehourhacks.blogspot.ie/2013/04/a-simple-python-plugin-framework.html 
The general idea is that you give the function a path then it iterates through the path, 
finding python files, loading them, and then searching them for any classes that are 
subclasses of the class you want. When it gets done, it returns them.
'''
from tools.log import Log; stdlog = Log.stdlog
from xdr import ndmp_const as const
import inspect, os, importlib
from functools import wraps
from distutils import spawn
from server.bu import Backup_Utility



def load_plugins(dirname):
    '''Loads a set of plugins at the given path.'''
    plugins = []
    plugin_dir = os.path.normpath(os.path.join(
                        os.path.abspath(os.path.join(
                                os.path.dirname(__file__),'..',dirname))))

    for f in os.listdir(plugin_dir):
        if f.endswith(".py"): 
            name = f[:-3]
            try:
                mod = importlib.import_module('bu.' + name)
                for (_, mod_piece) in inspect.getmembers(mod):
                    if (issubclass(mod_piece, Backup_Utility) and not mod_piece == Backup_Utility
                        and spawn.find_executable(mod_piece.executable)):
                        plugins.append(mod_piece)
            except (TypeError, ImportError):
                pass
    return plugins

def validate(func):
    '''
    A decorator that verify if the required plugin exists
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        record = args[1]
        for bu in record.bu_plugins:
            if record.b.bu_type == bu.butype_info.butype_name:
                record.data['bu'] = bu
        if not record.data['bu']:
            stdlog.error('BUTYPE ' + bytes(record.b.bu_type).decode() + 
                         ' not supported')
            record.error = const.NDMP_ILLEGAL_ARGS_ERR
            return
        else:
            return func(*args, **kwargs)
    return wrapper