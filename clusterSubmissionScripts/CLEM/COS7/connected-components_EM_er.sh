#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkConnectedComponents
N_NODES=10


ARGV="\
--inputN5DatasetName 'er' \
--outputN5DatasetSuffix '_ccSkipSmoothing' \
--minimumVolumeCutoff 25E6 \
--skipSmoothing \
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/imageData/COS7_Cell11.n5' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
