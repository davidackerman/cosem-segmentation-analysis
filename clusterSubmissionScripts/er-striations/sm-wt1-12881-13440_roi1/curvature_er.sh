#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

CLASS=org.janelia.cosem.analysis.SparkCurvature
N_NODES=1

cell=${PWD##*/}

ARGV="\
--inputN5Path '/nrs/cellmap/ackermand/cellmap/er-striations/${cell}.n5' \
--inputN5DatasetName 'er' "

export RUNTIME="48:00"
#export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
