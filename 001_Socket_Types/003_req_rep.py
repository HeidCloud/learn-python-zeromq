#!/usr/bin/env python2
# REQ and REP sockets work together to establish a synchronous bidirectional flow of data.
# You can think of REQ and REP much like you'd think of a protocol like HTTP, you send a request,
# and you get a response. In between the request and response the thread is blocked.
# 
# REQ sockets are load balanced among all clients, exactly like PUSH sockets. REP responses are
# correctly routed back to the originating REQ socket.
#
# To start, we're going to build a simple rep/req message system that looks like this:
#
#                          req_sock
#                             |
#                          rep_sock
#

import threading
import time
import zmq

class ReqSocket(threading.Thread):
    """ Definition of the request socket.

        It sends requests to the the response socket.
    """
    def __init__(self):
        """ Initialize the request socket. """
        threading.Thread.__init__(self)
        self.deamon=True                          # Deamon = True -> The thread finishes when the main process does

    def run(self):
        """ Send to the 2200 port the message 'Marco...' and wait for the response. """
        req_sock = context.socket(zmq.REQ)
        req_sock.connect("tcp://127.0.0.1:2200")
        req_sock.send("Marco...")
        response = req_sock.recv()
        print "Received response '" + response + "'"

class RepSocket(threading.Thread):
    """ Definition of the response socket.

        It binds the port 2200 and waits for a request. 
        Then it sends the response.
    """
    def __init__(self):
        """ Initialize the response socket. """
        threading.Thread.__init__(self)
        self.deamon=True                          # Deamon = True -> The thread finishes when the main process does
        
    def run(self):
        """ Binds the port 2200 and waits for requests. """
        rep_sock = context.socket(zmq.REP)
        rep_sock.bind("tcp://127.0.0.1:2200")
        message = rep_sock.recv()
        while message:
            print "Received request '" + message + "'"
            rep_sock.send("Polo!")
            message = rep_sock.recv()
            
        
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        start_time = time.clock()
        # init the REP socket thread
        thread_rep = RepSocket()
        thread_rep.start()

        # init the two REQ socket threads
        for i in [0,1]:
            thread_req = ReqSocket()
            thread_req.start()

        time.sleep(5)                              # Wait enough time to let the push-pull proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave



        
