#!/bin/bash

NUTCHDIR="/opt/nutch"
uclouds=(172.16.0.240 172.16.0.230 172.16.0.220)
depth=$1

for ucloud in ${uclouds[@]}
do
    ssh ${ucloud} "cd ${NUTCHDIR} && ./bin/dnutch $1" > ${NUTCHDIR}/ucloud_${ucloud}.txt &
done;
wait
