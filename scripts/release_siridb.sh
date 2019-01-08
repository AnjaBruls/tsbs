#!/bin/bash

if [ -d ~/workspace/dbtest/dbpath0/benchmark ]; then
  rm -r ~/workspace/dbtest/dbpath0/benchmark
fi

xterm -e ~/workspace/siridb-server/Release/siridb-server -c  ~/workspace/dbtest/siridb0.conf &




