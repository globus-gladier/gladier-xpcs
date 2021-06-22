#!/bin/bash
lines=0
while IFS= read -r line
do
    printf '%s\n' "${line}"
    eval $line
    sleep 15
done < "$1"
