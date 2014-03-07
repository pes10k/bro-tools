#!/bin/bash
STATE_FILE="/tmp/bro-state-$RANDOM.pickle"
NUM_FILES=`ls $1 | wc -l`
CURRENT_FILE=0
for FILE in `ls $1`; do 
    CURRENT_FILE=$(($CURRENT_FILE+1))
    echo "$CURRENT_FILE/$NUM_FILES: $FILE";
    zcat $FILE | python -O parse.py -d --time=.5 --state=$STATE_FILE --output=$2; 
done
