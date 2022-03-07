#!/bin/bash

OLDIFS=$IFS
IFS=
for ((i=1; i <= $2; i++)); do
  out=`go test -run $1 -race 2>&1`
  echo -e $out
  echo -e $out | grep 'warning\|exit\|FAIL'
  if [[ $? = 0 ]]; then
    echo "No.$i detect problem"
    break
  else
    echo "No.$i PASS"
  fi
done;
IFS=$OLDIFS