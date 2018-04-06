#!/bin/sh
cat nypd.csv | ./mapper.py | sort | ./reducer.py