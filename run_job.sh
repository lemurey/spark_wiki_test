#!/bin/bash
MONTH=02
for i in $(seq -f "%02g" 11 29); do
    if [ ! -d 02-$i ]; then
    mkdir $MONTH-$i
    fi
    for f in pagecounts-2016$MONTH$i*.gz; do
        STEM=$(basename "${f}" .gz)
        gunzip -c "${f}" > $MONTH-$i/"${STEM}"
        # rm "$f"
    done
    python process_wiki.py $MONTH-$i
    cat $MONTH-$i-processed/part-* > $MONTH-$i-processed/combined.txt
    rm -f /data/$MONTH-$i-processed/.part*
    rm -f /data/$MONTH-$i-processed/part*
done