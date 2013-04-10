import socketserver, socket, threading, traceback, struct, sys
from queue import Queue, Empty
from xdr.record import Record
from server.config import Config; cfg = Config.cfg
from server.log import Log; stdlog = Log.stdlog
import xdr.ndmp_const as const
from interfaces import notify


class RequestHandler(socketserver.BaseRequestHandler):
        
    def setup(self):
        # Create a new record for each connection
        self.record = Record()
        # Create a new queue for each connection
        self.wqueue = Queue()
        self.record.queue = self.wqueue
        self.wqueue.put_nowait(notify.connection_status().post(shutdown=False))
        
    def handle(self):
        try:
            while True:
                if not (self.wqueue.empty()):
                    self.send()
                else:
                    self.record.message = self.recv()
                    self.record.RECV()
                    if self.record.h.message in [const.NDMP_CONNECT_CLOSE, const.NDMP_SHUTDOWN]:
                        break
                    self.wqueue.put_nowait(self.record.SEND())
                # Cleanup self.record for next iteration
                self.record.reset()
                #input('next')
        except IOError:
            stdlog.info('Connection from ' + repr(self.client_address) + ' finished')
        except:
            stdlog.debug('*'*60)
            stdlog.debug(traceback.format_exc())
            stdlog.debug('*'*60)
            self.wqueue.put_nowait(notify.connection_status().post(shutdown=True))
        finally:
            self.record.close()
            self.finish()

    def finish(self):
        self.request.close()
        self.record = None
        
    def send(self):
        """Prepare and send messages using record marking standard"""
        while not self.wqueue.empty():
            try:
                message = self.wqueue.get()
            except Empty:
                stdlog.error('nothing to send')
                raise
            x = len(message) | 0x80000000
            header = struct.pack('>L', x | len(message))
            try:
                self.request.send(header + message)
            except socket.error as e:
                stdlog.error(repr(e))
                raise
            except (OSError, IOError) as e :
                stdlog.error(e.strerror)
                raise
    
    def recv(self):
        """Receive and unpack data using record marking standard"""
        last = False
        data = b''
        while not last:
            rec_mark = self._recv_all(4)
            count = struct.unpack('>L', rec_mark)[0]
            last = count & 0x80000000
            if last:
                count &= 0x7fffffff
            data += self._recv_all(count)
        return data
    
    def _recv_all(self, n):
        """Receive n bytes, or raise an error"""
        data = b''
        while n > 0:
            newdata = self.request.recv(n)
            count = len(newdata)
            if not count:
                raise IOError
            data += newdata
            n -= count
        return data


class NDMPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):

    allow_reuse_address = True
    request_queue_size = int(cfg['MAX_THREADS'])
    timeout = int(cfg['SOCKET_TIMEOUT'])
    
    def server_activate(self):
        stdlog.info('Starting NDMP server')
        super().server_activate()

    def handle_timeout(self):
        stdlog.error('Timeout')
        super().handle_timeout()
    
    def verify_request(self, request, client_address):
        stdlog.info('Received request from ' + repr(client_address))
        stdlog.debug('Active threads: ' + repr(threading.activeCount()))
        '''The first message sent on the connection MUST be an
            NDMP_NOTIFY_CONNECTION_STATUS message from the NDMP Server.'''
        return True
            
    def process_request(self, request, client_address):
        stdlog.debug('Processing Request')
        """Start a new thread to process the request,
        giving a meaningful name to the thread"""
        t = threading.Thread(name='Server-' + repr(threading.activeCount()),
                             target = self.process_request_thread,
                             args = (request, client_address))
        t.start()
        
    def close_request(self, request):
        stdlog.info('Request closed')
        
    def shutdown(self):
        stdlog.info('Closing NDMP server')
        super().shutdown()