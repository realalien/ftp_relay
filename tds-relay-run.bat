

@echo off
set RUNDIR=d:\wr\tds-relay-2017-12-18b\tds-relay
set DATDIR=d:\wr\tds-relay-2017-12-18b\tds-relay\tests\sandbox2


cd %RUNDIR%
python tds_relay.py -r %DATDIR% --delay 0 --no-validate-customer
