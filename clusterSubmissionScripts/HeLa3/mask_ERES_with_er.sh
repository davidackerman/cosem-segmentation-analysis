#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkExpandMaskToCleanPredictions
N_NODES=15

TRAININGPATH='setup03/HeLa_Cell3_4x4x4nm/HeLa_Cell3_4x4x4nm_it750000.n5'
INPUTN5PATH="/nrs/cosem/cosem/training/v0003.2/$TRAININGPATH"
OUTPUTN5PATH="/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/$TRAININGPATH"

ARGV="--keepWithinMask \
--datasetToMaskN5Path '$INPUTN5PATH' \
--datasetNameToMask 'ERES' \
--skipConnectedComponents \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa3.n5' \
--datasetNameToUseAsMask 'er' \
--expansion 0 \
--outputN5Path '$OUTPUTN5PATH' "

#export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
