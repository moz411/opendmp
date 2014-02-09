''' from http://onehourhacks.blogspot.ie/2013/04/a-simple-python-plugin-framework.html 
The general idea is that you give the function a path then it iterates through the path, 
finding python files, loading them, and then searching them for any classes that are 
subclasses of the class you want. When it gets done, it returns them.
'''
from tools.log import Log; stdlog = Log.stdlog
import inspect, os, importlib

class Plugins:
    pass

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
                for (mod_type, mod_piece) in inspect.getmembers(mod):
                    if issubclass(mod_piece, Plugins) and not mod_piece == Plugins:
                        plugins.append(mod_piece)
            except (TypeError, ImportError):
                pass 

    return plugins