'''This module implements the base server that receive NDMP request and prepare response.
Mostly from https://code.google.com/p/tulip/source/browse/examples/simple_tcp_server.py'''

import struct, asyncio, traceback
from tools.config import Config; cfg = Config.cfg; c = Config;
from tools.log import Log; stdlog = Log.stdlog
from xdr.record import Record
from interfaces import notify

class NDMPServer:
    
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.connections = {}
        
    def start(self):
        """
        Starts the TCP server, so that it listens on port 10000.

        For each client that connects, the accept_dma method gets
        called.  This method runs the loop until the server sockets
        are ready to accept connections.
        """
        handler = asyncio.start_server(self.accept,cfg['HOST'], int(cfg['PORT']))
        self.server = self.loop.run_until_complete(handler)
        stdlog.info('NDMP Server listening on %s:%d', cfg['HOST'], int(cfg['PORT']))
        self.loop.run_forever()
        
                
    def stop(self):
        """
        Stops the TCP server, i.e. closes the listening socket(s).
        This method runs the loop until the server sockets are closed.
        """
        if self.server is not None:
            self.server.close()
            self.loop.run_until_complete(self.server.wait_closed())
            self.server = None
                        
    def accept(self, reader, writer):
        '''
        This method accepts a new DMA connection and creates two Tasks
        to handle this connection.
        A notification is immediately sent to the DMA
        '''
        # Create a new Record for each connection
        self.record = Record()
        
        # Notify the DMA of the connection
        notify.connection_status().post(self.record)
        
        # start a new Task to handle this specific client connection
        asyncio.Task(self.handle_write(writer))
        read = asyncio.Task(self.handle_read(reader))
        
        # Add tasks and transports to the dict for reference
        self.connections[read] = (reader, writer)
        
        def handle_close(task):
            stdlog.info('Connection closed')
            self.record.close()
            del self.connections[task]
            
        # Add a handler to terminate the connection
        if read:
            read.add_done_callback(handle_close)

    @asyncio.coroutine
    def handle_read(self, reader):
        print('read')
        try:
            msglen = yield from reader.readexactly(4)
            print(msglen)
            count = struct.unpack('>L', msglen)[0]
            message = yield from reader.read(count)
        except asyncio.IncompleteReadError:
            stdlog.error('Incomplete message received')
        else:
            yield from self.record.run_task(message)
        
    @asyncio.coroutine        
    def handle_write(self, writer):
        print(self.record.queue.qsize())
        while not self.record.queue.empty():
            message = self.record.queue.get()
            x = len(message) | 0x80000000
            header = struct.pack('>L', x | len(message))
            try:
                yield from writer.write(header + message)
            except TypeError:
                pass

asyncio.Protocol