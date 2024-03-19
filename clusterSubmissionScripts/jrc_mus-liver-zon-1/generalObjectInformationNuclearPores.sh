#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation
N_NODES=10
export N_CORES_DRIVER=48
export N_EXECUTORS_PER_NODE=20
export N_CORES_PER_EXECUTOR=2
export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}


ARGV="\
--inputN5Path '/nrs/cellmap/ackermand/cellmap/${cell}.n5' \
--outputDirectory '/nrs/cellmap/ackermand/cellmap/analysisResults/${cell}' \
--inputN5DatasetName 'nuclear_pores' \
--skipContactSites "

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

