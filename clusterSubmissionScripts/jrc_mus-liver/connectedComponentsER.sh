#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=20
export LSF_PROJECT=cellmap
export N_CORES_DRIVER=48
#export N_CORES_PER_EXECUTOR=10
#-----------------------------------------------------------
# setup for 11 cores per worker (allows 4 workers to fit on one 48 core node with 4 cores to spare for other jobs)
export N_EXECUTORS_PER_NODE=20
export N_CORES_PER_EXECUTOR=2
# To distribute work evenly, recommended number of tasks/partitions is 3 times the number of cores.
#N_TASKS_PER_EXECUTOR_CORE=3
export N_OVERHEAD_CORES_PER_WORKER=4
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}

trainingPath="${cell}/${cell}.n5/evaluations-best/full"
inputN5Path="/nrs/cellmap/pattonw/predictions/$trainingPath"
outputN5Path="/groups/cellmap/cellmap/ackermand/cellmap/withFullPaths/$trainingPath"

dataset=er_maskedWith_plasma_membrane_expansion_40
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

if [ -L /groups/cellmap/cellmap/ackermand/cellmap/${cell}.n5/er ]; then
	unlink /groups/cellmap/cellmap/ackermand/cellmap/${cell}.n5/er
fi
ln -s $outputN5Path/${dataset}_cc /groups/cellmap/cellmap/ackermand/cellmap/${cell}.n5/er
