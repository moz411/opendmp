'''This module implements the base socket server that receive NDMP request
and fork a subprocess'''


from tools.config import Config; cfg = Config.cfg; c = Config;
from tools.log import Log; stdlog = Log.stdlog
import socket, struct, traceback
from xdr.record import Record
from interfaces import notify
import asyncore

class Server(asyncore.dispatcher):
    
    def __init__(self, connection):
        asyncore.dispatcher.__init__(self, connection)
        self.connection = connection
        # Create a new Record for each connection
        self.record = Record()
        # Notify the DMA of the connection
        notify.connection_status().post(self.record)
        
    def writeable(self):
        if self.record.queue.qsize() > 0:
            # A response is in the record's queue
            return True

    def handle_write(self):
        while not self.record.queue.empty():
            message = self.record.queue.get()
            if message:
                """Prepare and send messages using record marking standard"""
                x = len(message) | 0x80000000
                header = struct.pack('>L', x | len(message))
                self.send(header + message)

    def handle_read(self):
        """Receive and unpack message using record marking standard"""
        last = False
        message = b''
        while not last:
            rec_mark = self._recv_all(4)
            count = struct.unpack('>L', rec_mark)[0]
            last = count & 0x80000000
            if last:
                count &= 0x7fffffff
            message += self._recv_all(count)
        self.record.run_task(message)

    def handle_error(self):
        stdlog.info('Connection with ' + repr(self.addr) + ' failed')
        stdlog.debug(traceback.print_exc())
        self.close() # connection failed, shutdown
        
    def handle_close(self):
        stdlog.info('Connection with ' + repr(self.addr) + ' closed')
        self.close()
    
    def _recv_all(self, n):
        """Receive n bytes, or terminate connection"""
        data = b''
        while n > 0:
            newdata = self.recv(n)
            count = len(newdata)
            if not count:
                raise socket.error
            data += newdata
            n -= count
        return data

class NDMPServer(asyncore.dispatcher):

    def __init__(self, hostname, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((hostname, port))
        self.listen(1)
            
    def handle_accepted(self, connection, address):
        stdlog.info('Connection from ' + repr(address))
        # Start a asyncore Consumer for this connection
        Server(connection)


