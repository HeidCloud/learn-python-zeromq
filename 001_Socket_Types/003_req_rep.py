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

def req_function():
    """ Definition of the request socket.

        It sends requests to the the response socket.
    """
    req_sock = context.socket(zmq.REQ)
    req_sock.connect("inproc://simplereqrep")
    req_sock.send("Marco...")
    response = req_sock.recv()
    print "Received response '" + response + "'"

def rep_function():
    """ Definition of the response socket.

        It binds the port 2200 and waits for a request. 
        Then it sends the response.
    """
    rep_sock = context.socket(zmq.REP)
    rep_sock.bind("inproc://simplereqrep")
    message = rep_sock.recv()
    while message:
        print "received request '" + message + "'"
        rep_sock.send("Polo!")
        message = rep_sock.recv()
        
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        start_time = time.clock()
        # init the REP socket thread
        thread_rep = threading.Thread(target=rep_function)
        thread_rep.start()

        # init the two REQ socket threads
        for i in [0,1]:
            thread_req = threading.Thread(target=req_function)
            thread_req.start()

        time.sleep(5)                              # Wait enough time to let the push-pull proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave
        
