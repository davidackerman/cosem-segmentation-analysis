#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMicrotubules
N_NODES=3

for i in  {groundtruth,reconstructed}; do
ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/supplementNewestMicrotubules/${i}.n5' \
--inputN5DatasetName 'hela_3_block_1,hela_3_block_2,jurkat_block_1,jurkat_block_2,macrophage_block_1,macrophage_block_2' \
--innerRadiusInNm 6 \
--outerRadiusInNm 12.5 \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

sleep 2
done
