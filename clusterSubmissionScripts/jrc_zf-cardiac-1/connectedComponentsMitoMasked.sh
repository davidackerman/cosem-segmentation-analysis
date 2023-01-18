#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}

trainingPath="for_aubrey/jrc_zf-cardiac-1.n5/"
inputN5Path="/nrs/cellmap/bennettd/$trainingPath"
outputN5Path="/nrs/cellmap/ackermand/jrc_zf-cardiac-1/withFullPaths/$trainingPath"

dataset="mito_masked"
mkdir -p $outputN5Path
unlink $outputN5Path/$dataset
ln -s $inputN5Path/$dataset/s0 $outputN5Path/$dataset

outputN5DatasetSuffix="_cc"

ARGV="\
--inputN5DatasetName $dataset \
--minimumVolumeCutoff 20E6 \
--outputN5DatasetSuffix _cc \
--inputN5Path $outputN5Path \
--outputN5Path $outputN5Path \
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

if [ -L /nrs/cellmap/ackermand/${cell}/${cell}.n5/$dataset ]; then
	unlink /nrs/cellmap/ackermand/${cell}/${cell}.n5/$dataset
fi
ln -s $outputN5Path/${dataset}_cc /nrs/cellmap/ackermand/${cell}/${cell}.n5/$dataset
