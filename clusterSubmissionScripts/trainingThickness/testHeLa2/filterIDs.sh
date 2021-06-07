#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis' #`dirname "${BASH_SOURCE[0]}"`
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
CLASS=org.janelia.cosem.analysis.SparkIDFilter

ARGV="--inputN5Path '/groups/cosem/cosem/data/jrc_hela-2/jrc_hela-2.n5/volumes/groundtruth/0003/crop3/labels/' \
--inputN5DatasetName 'all' \
--outputN5Path '/groups/cosem/cosem/ackermand/testTrainingThickness.n5' \
--outputN5DatasetSuffix '_2' \
--idsToKeep '2' "
# --idsToKeep '2,3,6,8,10,12,14,16,18,20,22'"

N_NODES=5
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
