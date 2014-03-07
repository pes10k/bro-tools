#!/bin/bash
STATE_FILE="/tmp/bro-state-$RANDOM.pickle"

for FILE in `ls $1`; do 
    echo $FILE; 
    zcat $FILE | python -O parse.py -d --time=.5 --state=$STATE_FILE --output=$2 -v; 
done
