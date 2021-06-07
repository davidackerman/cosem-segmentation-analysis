#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis' #`dirname "${BASH_SOURCE[0]}"`
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
CLASS=org.janelia.cosem.analysis.SparkCalculateThicknessHistograms

ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/testTrainingThickness.n5' \
--inputN5DatasetName 'all_2' "

N_NODES=5
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
