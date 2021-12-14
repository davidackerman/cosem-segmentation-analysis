#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConnectedComponents
N_NODES=10

export LSF_PROJECT=cosem
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
export N_EXECUTORS_PER_NODE=2

cell=${PWD##*/}

mkdir -p /groups/cosem/cosem/ackermand/cosem/${cell}.n5/
while IFS=, read -r dataset iteration minimumVolumeCutoff skipSmoothing
do
	trainingPath="setup04/$cell/foreground_s0/${cell}_it${iteration}.n5"
	inputN5Path="/nrs/cosem/cosem/training/v0003.2/$trainingPath"
	outputN5Path="/groups/cosem/cosem/ackermand/cosem/withFullPaths/$trainingPath"
	
	mkdir -p $outputN5Path
        unlink $outputN5Path/$dataset
	ln -s $inputN5Path/$dataset/s0 $outputN5Path/$dataset

	outputN5DatasetSuffix="_cc"
	skipSmoothingFlag=""
	if [ "$skipSmoothing" == "--skipSmoothing" ]; then
		skipSmoothingFlag=$skipSmoothing
		outputN5DatasetSuffix="_ccSkipSmoothing"
	fi

	ARGV="\
	--inputN5DatasetName $dataset \
	--minimumVolumeCutoff $minimumVolumeCutoff \
	--outputN5DatasetSuffix $outputN5DatasetSuffix \
	--inputN5Path $outputN5Path \
	--outputN5Path $outputN5Path \
	$skipSmoothingFlag
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
	sleep 1
	if [ ! -L /groups/cosem/cosem/ackermand/cosem/${cell}.n5/$dataset ]; then
		ln -s $outputN5Path/${dataset}${outputN5DatasetSuffix} /groups/cosem/cosem/ackermand/cosem/${cell}.n5/$dataset
	fi

done < iterationsAndParameters.csv
