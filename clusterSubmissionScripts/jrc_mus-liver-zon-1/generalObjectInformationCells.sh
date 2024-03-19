#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation
N_NODES=10
export N_CORES_DRIVER=4
export N_EXECUTORS_PER_NODE=1
export N_CORES_PER_EXECUTOR=10
# export N_OVERHEAD_CORES_PER_WORKER=8
export N_OVERHEAD_CORES_PER_WORKER=1

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}


ARGV="\
--inputN5Path '/nrs/cellmap/ackermand/cellmap/${cell}.n5' \
--outputDirectory '/nrs/cellmap/ackermand/cellmap/analysisResults/${cell}' \
--inputN5DatasetName 'cells' \
--skipContactSites "

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

