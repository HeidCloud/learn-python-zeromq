#!/usr/bin/env python2
#
# This example expands the features of the previous PubSub exercise. One of the characteristic of
# this kind of socket pattern is that the Sub socket can lose packets because it starts to listen
# at the channel once the Pub socket has already started to publish thought it.
# Here, we intend to create an unidirectional PubSub comunication, but only when all the actors in scene are ready.
#
# We are going to implement a classroom. The teacher does not start the class until all the students are inside and
# ready to listen. 
# In a socket diagram, this could be:
#
#
#           -----------------------------------------------------
#           -                 teacher                           -
#           -----------------------------------------------------
#           -    Pub socket         |       Rep socket          -
#           -----------------------------------------------------
#                      |                   ^              | 
#                      |                   |              | 
#                      |                   |              | 
#                  lesson channel      'Hi teacher   'Hi student
#                      |                 channel'       channel'
#                      |                   |              |
#                      |                   |              |
#                      <                   |              <
#           -----------------------------------------------------
#           -   Sub socket          |      Req socket           -
#           -----------------------------------------------------
#           -                student                            -
#           -----------------------------------------------------
#
# To do it more realistic, we are going to use processes instead of threads.This is the student part of the example.

import threading
import time
import zmq

def ready_to_learn(student_num):
    """ Connect to teacher to say 'Hello' and make him realise you are ready.
    """ 

    # As we know the publisher won't start to publish until all the 
    # subscriber are ready, first start to listen for the pub-sub channel
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://127.0.0.1:2202")
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    # Then say 'hello' to teacher through the rep-req channel
    req_sock = context.socket(zmq.REQ)
    req_sock.connect("tcp://127.0.0.1:2201")
    print "Student " + str(student_num) + " say 'hello' to teacher";
    req_sock.send("Hello Teacher!");
    req_sock.recv()


    # Print the lesson learned
    while True:
        lesson = subscriber.recv()
        if (lesson == 'END'):
            break
        print "Student " + str(student_num) + " thoughts : {" + lesson + "}"
    
if __name__ == '__main__':

    context = zmq.Context()
    try:                                           
        # init the three REP sockets thread
        thread_rep = threading.Thread(target=ready_to_learn, args=(1, ))
        thread_rep.start()
        time.sleep(3)

        thread_rep = threading.Thread(target=ready_to_learn, args=(2, ))
        thread_rep.start()
        time.sleep(4)

        thread_rep = threading.Thread(target=ready_to_learn, args=(3, ))
        thread_rep.start()
        time.sleep(2)

        time.sleep(5)                              # Wait enough time to let the lessond end proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"
    finally:
        context.term()                                      # End the ZeroMQ context before to leave

