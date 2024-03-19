#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=20
export N_CORES_DRIVER=48
export N_EXECUTORS_PER_NODE=10
export N_CORES_PER_EXECUTOR=4
export N_OVERHEAD_CORES_PER_WORKER=8
export N_TASKS_PER_EXECUTOR_CORE=100
#export OVERRIDE_PARALLELISM=400
export LSF_PROJECT=cellmap
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}
dataset="er_blockSize128"
trainingPath="jrc_mus-liver-zon-1/jrc_mus-liver-zon-1.n5/predictions/2023-04-10/er/best/"
inputN5Path="/nrs/cellmap/pattonw/predictions/$trainingPath"
outputN5Path="/nrs/cellmap/ackermand/cellmap/withFullPaths/$trainingPath"

#mkdir -p $outputN5Path
#if [ -L $outputN5Path/${dataset} ]; then
#        unlink $outputN5Path/${dataset}
#fi
#ln -s $inputN5Path $outputN5Path/${dataset}

if [ -L "/nrs/cellmap/ackermand/cellmap/${cell}.n5/er" ]; then
	unlink "/nrs/cellmap/ackermand/cellmap/${cell}.n5/er"
fi

ln -s $outputN5Path/${dataset}_cc "/nrs/cellmap/ackermand/cellmap/${cell}.n5/er"



outputN5DatasetSuffix="_cc"

ARGV="\
--inputN5DatasetName $dataset \
--minimumVolumeCutoff 20E6 \
--outputN5DatasetSuffix _cc \
--inputN5Path $outputN5Path \
--outputN5Path $outputN5Path \
--skipSmoothing \
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

# if [ -L /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset ]; then
#	unlink /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset
# fi
# ln -s $outputN5Path/${dataset}_cc /nrs/cellmap/ackermand/cellmap/${cell}.n5/$dataset
