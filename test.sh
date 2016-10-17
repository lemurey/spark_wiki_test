#!/bin/bash
for i in $(seq -f "%02g" 1 31); do
    if [ ! -d 02-$i ]; then
    mkdir 02-$i
    fi
    for f in pagecounts-201602$i*.gz; do
        STEM=$(basename "${f}" .gz)
        gunzip -c "${f}" > 02-$i/"${STEM}"
        # rm "$f"
    done
    python process_wiki.py 02-$i
    cat 02-$i-processed/part-* > 02-processed/combined-$i.txt
done






