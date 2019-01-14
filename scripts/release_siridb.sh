#!/bin/bash


xterm -e valgrind --tool=memcheck ~/workspace/siridb-server/Debug/siridb-server -c  ~/workspace/dbtest/siridb0.conf &
