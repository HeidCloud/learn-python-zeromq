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
# To do it more realistic, we are going to use processes instead of threads.This is the teacher part of the example.
import time
import zmq

context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://127.0.0.1:2202")

NUMBER_OF_STUDENTS = 3
rep_socket = context.socket(zmq.REP)
rep_socket.bind("tcp://127.0.0.1:2201")

print "Waiting for my " + str(NUMBER_OF_STUDENTS) + " students to start the lesson..."
students_in_class = 0

# Waiting for students to start..
while students_in_class < NUMBER_OF_STUDENTS:
    string = rep_socket.recv()
    students_in_class = students_in_class + 1
    print "I received a '" + string + "'. There are " + str(students_in_class) + " students in class."
    rep_socket.send("")

print "Everyone in ready. Start the lesson..."
publisher.send("En un lugar de la Mancha, de cuyo nombre no quiero acordarme, ")
publisher.send(" no ha mucho tiempo que vivia un hidalgo de los de lanza en astillero ")
publisher.send(" adarga antigua, rocin flaco y galgo corredor. ")
publisher.send('END')
time.sleep(10)

context.term()                                      # End the ZeroMQ context before to leave

