#!/bin/bash

#
# Environment Configuration Variables
#
PYTHON=/usr/local/bin/python3
RUNDIR=/home/pdfs/tds-relay
DATDIR=/home/pdfs/data

#
# Do not change anything below this line.
# 
cd ${RUNDIR} 
${PYTHON} tds_relay.py -r ${DATDIR} --delay 0
