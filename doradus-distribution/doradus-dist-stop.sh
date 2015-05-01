#!/bin/bash
# The logic to stop cassandra and doradus
set -x
kill `ps -ef | grep cassandra | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
kill `ps -ef | grep doradus | grep -v grep | awk '{ print $2 }'` > /dev/null 2>&1
exit 0