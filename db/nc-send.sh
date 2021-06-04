#!/bin/sh
set -eu

HOST=thetalogin6
PORT=5555

echo hi | nc $HOST $PORT
