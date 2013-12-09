"""
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

import sys, traceback, faulthandler
from tools.log import Log
from tools.config import Config
from tools.daemon import Daemon
from server.server import NDMPServer


class MyDaemon(Daemon):
    def run(self):
        while True:
            server = NDMPServer(cfg['HOST'], int(cfg['PORT']))
            try:
                server.start()
            except:
                stdlog.debug('*'*60)
                stdlog.debug(traceback.format_exc())
                faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                stdlog.debug('*'*60)

# get config
try:
    cfg = Config().getcfg('opendmp.conf')
except:
    print(traceback.format_exc().splitlines()[-1])
    print('Something wrong with configuration, exiting')
    sys.exit(1)
    
# get local logging
try:
    stdlog = Log().getlog()
except:
    print(traceback.format_exc().splitlines()[-1])
    print('Something wrong with logfile, exiting')
    sys.exit(1)
    
if __name__ == "__main__":
    daemon = MyDaemon('/var/run/opendmp/daemon.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
        
    
