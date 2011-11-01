#!/usr/bin/env python2
# This example expands the features of the previous single REQ-REP exercise. A REQ socket can be connected
# to multiple REP sockets. The requests from the REQ socket are load-balaced to all REP sockets. 
#
# In this example we are going to connect a single REQ socket to multiple REP sockets to see this
# behaviour.
#
#                          req_sock
#                             |
#                 -------------------------
#                 |           |           |
#              rep_sock    rep_sock    rep_sock
#
import threading
import time
import zmq

def req_function(ports):
    """ Definition of the request socket.

        It sends requests to the the response sockets.
    """
    req_sock = context.socket(zmq.REQ)
    for port in ports:                       # initialize the connection to all the ports.
        print "ReqSocket connecting to port " + str(port)
        req_sock.connect("tcp://127.0.0.1:" + str(port))

    for i in range(0, 10):
        req_sock.send("Marco...")
        response = req_sock.recv()
        print response

def rep_function(identifier, port):
    """ Definition of the response socket.

        It binds a port and waits for a request. 
        Then it sends the response.
    """
    rep_sock = context.socket(zmq.REP)
    print "RepSocket " + str(identifier) + " binding port " + str(port) + "."
    rep_sock.bind("tcp://127.0.0.1:" + str(port))
    message = rep_sock.recv()
    while message:
        print "Received request '" + message + "'"
        rep_sock.send("RepSocket " + str(identifier) + " says: Polo!")
        message = rep_sock.recv()
            
        
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        # init the three REP sockets thread
        thread_rep = threading.Thread(target=rep_function, args=(1, 2201))
        thread_rep.start()

        thread_rep = threading.Thread(target=rep_function, args=(2, 2202))
        thread_rep.start()

        thread_rep = threading.Thread(target=rep_function, args=(3, 2203))
        thread_rep.start()

        # init the REQ socket 
        thread_req = threading.Thread(target=req_function, args=([2201, 2202, 2203], ))
        thread_req.start()

        time.sleep(25)                              # Wait enough time to let the push-pull proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave

