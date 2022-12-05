#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkExpandMaskToCleanPredictions
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"


cell=${PWD##*/}

trainingPath="${cell}/${cell}.n5/evaluations-best/full"
inputN5Path="/nrs/cellmap/pattonw/predictions/$trainingPath"
outputN5Path="/groups/cellmap/cellmap/ackermand/cellmap/withFullPaths/$trainingPath"

datasetNameToMask="er"
mkdir -p $outputN5Path
ln -s $inputN5Path/$datasetNameToMask $outputN5Path/$datasetNameToMask


ARGV="
--datasetToMaskN5Path $outputN5Path \
--datasetNameToMask $datasetNameToMask \
--skipConnectedComponents \
--datasetToUseAsMaskN5Path '/groups/cellmap/cellmap/ackermand/cellmap/${cell}.n5' \
--datasetNameToUseAsMask 'plasma_membrane' \
--expansion 40 \
--outputN5Path $outputN5Path "

#export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
