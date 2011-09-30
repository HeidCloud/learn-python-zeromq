#!/usr/bin/env python2

# PUSH and PULL sockets work together to load balance messages going one way.
# Multiple PULL sockets connected to a PUSH each receive messages from the PUSH.
# ZeroMQ automatically load balances the messages between all pull sockets.
#
# We're going to build a simple load balanced message system that looks like this:
#
#                         push_sock
#                         /       \
#                  pull_sock1   pull_sock2
#
# Each socket will get its own thread, so you'll see them run simultanously

import threading
import time
import zmq

def pull_function(num):
        """
            It recieves the messages from the push socket and prints them.
        """
        pull_sock = context.socket(zmq.PULL)
        time.sleep(1)
        print "Pull " + str(num) + " connecting"
        pull_sock.connect("inproc://push-pull-queue")
        message = pull_sock.recv()
        while message:
            print "Pull" + str(num) + ": I recieved a message '" + message + "'"
            message = pull_sock.recv()

def push_function():
    """ Definition of the function that executes the push socket. 
    
        It sends the message 'N Potato' as a load balancer
        to all the Pull sockets.
    """
    push_sock = context.socket(zmq.PUSH)
    push_sock.bind("inproc://push-pull-queue")
    for i in range (0, 7):
        message = str(i + 1) + " Potato"
        print "Push: Sending " + message
        message = push_sock.send(message)
        time.sleep(1)                           # Without this sleep, all the messages go to Pull0. That means that
                                                # zeromq assign the pull target switching by time, not by message.
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        start_time = time.clock()
        # init the push socket thread
        thread_push = threading.Thread(target=push_function)
        thread_push.start()

        # init the two pull socket threads
        for i in [0,1]:
            thread_pull = threading.Thread(target=pull_function, args=(i, ))
            thread_pull.start()

        time.sleep(15)                              # Wait enough time to let the push-pull proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave

