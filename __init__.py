""""
    Copyright (c) 2013 Thomas DUPOUY <moz@gmx.fr>.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, 
    this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright 
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

    3. Neither the name of Thomas DUPOUY nor the names of other contributors may 
    be used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import sys, traceback, asyncore, argparse
from server.config import Config
from server.log import Log
from server.server import NDMPServer, RequestHandler
from tools import daemon, lockfile
from tools.daemon import pidlockfile


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--daemon", action="store_true", help="Run as daemon")
args = parser.parse_args()

try:
    cfg = Config('opendmp.conf').getcfg()
except:
    print(traceback.format_exc().splitlines()[-1], file=sys.stderr)
    print('Something wrong with configuration, exiting', file=sys.stderr)
    sys.exit(1)

try:
    stdlog = Log().getlog()
except:
    print(traceback.format_exc().splitlines()[-1], file=sys.stderr)
    print('Something wrong with logfile, exiting', file=sys.stderr)
    sys.exit(1)
    

server = NDMPServer((cfg['HOST'], int(cfg['PORT'])), RequestHandler)

if args.daemon:
    context = daemon.DaemonContext(
        working_directory='/var/lib/opendmp',
        umask=0o002,
        pidfile=lockfile.FileLock('/var/run/opendmp.pid'),
        )
    
    context.signal_map = {
        signal.SIGTERM: server.shutdown(),
        signal.SIGHUP: 'terminate'
    }
    
    with daemon.DaemonContext():
            server.serve_forever()
else:
    try:
        server.serve_forever()
    except:
        stdlog.error(sys.exc_info()[1])
    finally:
        server.shutdown()