'''This module implements the base socket server that receive NDMP request
and fork a subprocess'''


from tools.config import Config; cfg = Config.cfg; c = Config;
from tools.log import Log; stdlog = Log.stdlog
import struct, traceback, faulthandler, sys, asyncio
from xdr.record import Record
from interfaces import notify

def start_NDMPServer():
    try:
        loop = asyncio.get_event_loop()
        coro = loop.create_server(NDMPServer, cfg['HOST'], int(cfg['PORT']))
        server = loop.run_until_complete(coro)
        stdlog.info('Start NDMP server on {}'.format(server.sockets[0].getsockname()))
        
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            stdlog.info("Exiting")
        finally:
            server.close()
            loop.close()
    except:
        stdlog.debug('*'*60)
        stdlog.debug(traceback.format_exc())
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        stdlog.debug('*'*60)
        
class NDMPServer(asyncio.Protocol):
    MAX_LENGTH = 99999
    recvd = b''
    prefixLength = 4
    structFormat = '>L'
            
    def connection_made(self, transport):
        self.transport = transport
        stdlog.info('Connection from ' + repr(self.transport.get_extra_info('peername')))
        # Create a new Record for each connection
        self.record = Record()
        # Send the initial welcome message
        task = asyncio.async(notify.connection_status().post(self.record))
        task.add_done_callback(self.handle_write)
        
    def data_received(self, data):
        '''Receive and unpack message using record marking standard'''
        self.recvd = self.recvd + data
        while len(self.recvd) >= self.prefixLength:
            length = struct.unpack(self.structFormat, self.recvd[:self.prefixLength])[0]
            message = self.recvd[self.prefixLength:length + self.prefixLength]
            self.recvd = self.recvd[length + self.prefixLength:]
            task = asyncio.async(self.record.run_task(message))
            task.add_done_callback(self.handle_write)
            
    def connection_lost(self, exc):
        stdlog.info(repr(self.transport.get_extra_info('peername')) + ' closed the connection')
        self.transport.close()

    def handle_write(self,task):
        ''''Prepare and send messages using record marking standard'''
        # Retrieve the formated NDMP message from the asyncio Task
        try:
            data = task.result()
        except EOFError:
            # XDR error, close connection
            stdlog.error(repr(self.transport.get_extra_info('peername')) + ': XDR Error')
            self.transport.close()
        else:
            # Prepare the XDR header
            x = len(data) | 0x80000000
            header = struct.pack(self.structFormat, x | len(data))
            # Send the message
            self.transport.write(header + data)