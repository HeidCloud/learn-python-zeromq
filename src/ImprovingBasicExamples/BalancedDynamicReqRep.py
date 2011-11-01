#!/usr/bin/env python2
# This example expands the features of the previous balanced REQ-REP (003b) exercise. 
#
# The 003b_balanced_req_rep works well balancing an amount of requests through multiple server REP sockets,
# but that defines an static infrastructure. If we wanted to add a fourth REP socket, we would need to 
# stop the process and parametrize the input parameters of the request socket.
#
# That's not good for a production environment. We need a more dynamic way to process the synchronized
# REQ/REP connections. ROUTER/DEALER sockets to the rescue.
#
# ROUTER/DEALER sockets (previously XREP/XREQ sockets) allows you to create a non-blocking request-response
# broker that manages the REP/REQ connection.
#
# In this example we are going to connect two REQ socket to a ROUTER socket which is listening the frontend part of the broker. 
# These requests are going to be forwarded to the backend part of the broker, managed by the DEALER. The DEALER
# is the responsible to load-balance the calls to the REP sockets.
#
# The responses send by the REP sockets will follow the same path but in the opposite direction.
#
#                     req_sock      req_sock
#                        |             |  
#                        ---------------
#                               |
#                          router_sock
#                         (broker code)
#                          dealer_sock 
#                               |
#                   ------------------------------------
#                   |           |           |          |
#                rep_sock    rep_sock    rep_sock  (rep_sock)
#
# To ilustrate this is a dynamic infrastructure, we have put an attribute in the response sockets that informs
# how many times the socket can respond to requests. When the Response socket arrives to the number of times
# it can respond, it dies.
#

import threading
import time
import zmq

def req_function(broker_router_uri, identifier):
    """ Definition of the request socket.

        It sends requests to the the router frontend of the broker.
    """
    req_sock = context.socket(zmq.REQ)
    print "ReqSocket " + str(identifier) + " connecting to broker router uri " + broker_router_uri
    req_sock.connect(broker_router_uri)

    for i in range(0, 10):
        message_to_send = "ReqSocket " + str(identifier) + " says: Hi!"  # Greet the Response socket
        print message_to_send
        req_sock.send(message_to_send)
        response = req_sock.recv()
        print response
        time.sleep(1)                                                    # Wait a second before to send the next message

def rep_function(broker_dealer_uri, identifier, times_greet):
    """ Definition of the response socket.

        It conects to broker's dealer uri and waits for a request. 
        Then it sends the response.
    """
    rep_sock = context.socket(zmq.REP)
    print "RepSocket " + str(identifier) + " connecting to broker dealer uri " + broker_dealer_uri
    rep_sock.connect(broker_dealer_uri)
    while times_greet > 0:                  # Maintain the loop while the times_greet is bigger than 0
        message = rep_sock.recv()
        rep_sock.send("RepSocket " + str(identifier) + " says: Hi '" + str.split(message)[1] + "'!")
        times_greet = times_greet - 1   # Decrease the times of remaining greet
    print "RepSocket " + str(identifier) + " says: Too many greets, gonna die..."
            
def broker(broker_router_uri, broker_dealer_uri):
    """ Definition of the broker queue.

    A broker queue binds the backend and the frontend URIs and sends the messages from one part to another
    REP/REQ socket use it to get dynamic infrastructure for its synchronized messaging."""
    frontend = context.socket(zmq.ROUTER)        # Define the ZMQ socket that will act as ROUTER(XREP)
    frontend.bind(broker_router_uri)

    backend = context.socket(zmq.DEALER)         # Define the ZMQ socket that will act as DEALER(XREQ)
    backend.bind(broker_dealer_uri)

    zmq.device(zmq.QUEUE, frontend, backend)     # using the builtin QUEUE device to do the job

    frontend.close()
    backend.close()

        
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        # init the three REP sockets thread
        frontend_broker_uri = "inproc://broker_frontend"
        backend_broker_uri = "inproc://broker_backend"
        
        # Initialize the broker
        queue_broker = threading.Thread(target=broker, args=(frontend_broker_uri, backend_broker_uri))
  
        # Initialize the ReqSockets
        req1 = threading.Thread(target=req_function, args=(frontend_broker_uri, 1))
        req2 = threading.Thread(target=req_function, args=(frontend_broker_uri, 2))

        # Initialize the ReqSockets
        rep1 = threading.Thread(target=rep_function, args=(backend_broker_uri, 1, 1))
        rep2 = threading.Thread(target=rep_function, args=(backend_broker_uri, 2, 2))
        rep3 = threading.Thread(target=rep_function, args=(backend_broker_uri, 3, 100000))
        
        queue_broker.start()

        rep1.start()
        rep2.start()
        rep3.start()

        req1.start()
        req2.start()

        time.sleep(5)                              # Wait enough time to let the push-pull proceses end

        # After 2 seconds, we add another RepSocket...
        rep4 = threading.Thread(target=rep_function, args=(backend_broker_uri, 4, 100000))
        rep4.start()

        time.sleep(15)

    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave
