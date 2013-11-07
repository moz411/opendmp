'''This module implements the base socket server that receive NDMP request
and fork a subprocess'''


from tools.config import Config; cfg = Config.cfg; c = Config; threads = Config.threads
from tools.log import Log; stdlog = Log.stdlog
import threading, queue, socket, traceback, struct, sys, select, os, time, multiprocessing
from xdr.record import Record
import xdr.ndmp_const as const
from interfaces import notify

class Consumer(threading.Thread):
    
    def __init__(self, connection, address):
        threading.Thread.__init__(self)
        self.connection = connection
        self.address = address
        # Create a new Record for each connection
        self.record = threading.local()
        self.record = Record()
        self.task_queue = queue.Queue()
        self.record.queue = queue.Queue()
        # Notify the DMA of the connection
        notify.connection_status().post(self.record)

    def run(self):
        try:
            while self.connection:
                time.sleep(0.001)
                (read, write, error) = select.select([self.connection], [self.connection], [])
                if write:
                    while not self.record.queue.empty():
                        message = self.record.queue.get()
                        if message: sendXDR(self.connection, message) 
                if read:        
                    message = recvXDR(self.connection)
                    self.task_queue.put(message)
                    answer = self.record.run_task(message)
                    self.record.queue.put(answer)
                if error:
                    break
        except socket.error:
            stdlog.info('Connection with ' + repr(self.address) + ' closed')
        finally:
            self.connection.close()
            sys.exit()

class NDMPServer(object):
    
    threads = []
    
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        stdlog.info('Starting NDMP server')
 
    def start(self):
        # Start a thread that will cleanup zombie children
        t = threading.Thread(target=self.cleanup_processes)
        t.start()
        
        # Start the initial socket listening on port 10000
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.hostname, self.port))
        server.listen(1)
        
        while True:
            # Wait for connection
            connection, address = server.accept()
            # Someone have connected to port 10000
            stdlog.info('Connection from' + repr(address))
            
            # Start the process that will handle this communication
            # This new process will be a DATASERVER or TAPESERVER
            p = Consumer(connection, address)
            p.start()
            global threads
            threads.append(p)
                    
                    
            
    def cleanup_processes(self):
        '''Will loop and join the threads and subprocesses'''
        while True:
            time.sleep(0.1)
            for thread in threads:
                thread.join(1)
            multiprocessing.active_children()
        
    def handle(rqueue, wqueue):
        stdlog.info('Started process ' + repr(os.getpid()))
        # Create a new record for each connection
        record = Record(wqueue)
        # Notify the DMA of the connection
        notify.connection_status().post(record)
            
        try:
            while True:
                record.message = rqueue.get()
                print(record.message)
                record.RECV()
                if record.h.message in [const.NDMP_CONNECT_CLOSE, const.NDMP_SHUTDOWN]:
                    break
                record.SEND()
                # Cleanup self.record for next iteration
                record.reset()
        finally:
            record.close()
            return
        
    
def sendXDR(connection, message):
        """Prepare and send messages using record marking standard"""
        x = len(message) | 0x80000000
        header = struct.pack('>L', x | len(message))
        try:
            connection.send(header + message)
        except OSError as e :
            stdlog.error(e)
            raise
        
def recvXDR(connection):
    """Receive and unpack data using record marking standard"""
    last = False
    data = b''
    while not last:
        rec_mark = _recv_all(connection, 4)
        count = struct.unpack('>L', rec_mark)[0]
        last = count & 0x80000000
        if last:
            count &= 0x7fffffff
        data += _recv_all(connection, count)
    return data

def _recv_all(connection, n):
    """Receive n bytes, or raise an error"""
    data = b''
    while n > 0:
        newdata = connection.recv(n)
        count = len(newdata)
        if not count:
            raise socket.error
        data += newdata
        n -= count
    return data
