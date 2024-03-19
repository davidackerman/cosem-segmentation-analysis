#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=30

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}
dataset="nucleus"
trainingPath="jrc_mus-liver-zon-1/jrc_mus-liver-zon-1.n5/predictions/2023-04-10/${dataset}/best/"
inputN5Path="/nrs/cellmap/pattonw/predictions/$trainingPath"
outputN5Path="/nrs/cellmap/ackermand/cellmap/withFullPaths/$trainingPath"

#mkdir -p $outputN5Path
#ln -s $inputN5Path $outputN5Path/$dataset

outputN5DatasetSuffix="_cc"

# minimum volume cutoff is 1.5 um x 1.5 um x 1.5 um
ARGV="\
--inputN5DatasetName $dataset \
--minimumVolumeCutoff 3.375E9 \
--maximumVolumeCutoff 0.25E13 \
--outputN5DatasetSuffix _ccMinMaxFilter \
--inputN5Path $outputN5Path \
--outputN5Path $outputN5Path \
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

# if [ -L /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset ]; then
#	unlink /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset
# fi
# ln -s $outputN5Path/${dataset}_cc /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset
