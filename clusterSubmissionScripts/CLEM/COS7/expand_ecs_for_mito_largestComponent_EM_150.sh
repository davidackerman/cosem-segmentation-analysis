#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkExpandMaskToCleanPredictions
N_NODES=10

export LSF_PROJECT=cosem
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
#export N_EXECUTORS_PER_NODE=2


ARGV="--datasetToMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/COS7_Cell11.n5' \
--datasetNameToMask 'mito' \
--onlyKeepLargestComponent \
--datasetToUseAsMaskN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/COS7_Cell11.n5' \
--datasetNameToUseAsMask 'ecs_largestComponent' \
--skipConnectedComponents \
--expansion 150 \
--outputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/COS7_Cell11.n5'"

export MEMORY_PER_NODE=500
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
