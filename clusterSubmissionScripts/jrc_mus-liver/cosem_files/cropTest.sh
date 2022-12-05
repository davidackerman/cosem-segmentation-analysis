#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkCrop
N_NODES=1

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
#export N_EXECUTORS_PER_NODE=2

cell=${PWD##*/}

	
	ARGV="\
	--outputN5DatasetSuffix _tmp \
	--inputN5DatasetName mito_cropped \
	--inputN5Path /groups/cosem/cosem/ackermand/cosem/withFullPaths/training/finetuning/jrc_mus-liver/liver_latest_setup04_many_masked_6-1_100000.n5 \
	--outputN5Path /groups/cosem/cosem/ackermand/liver_crop.n5 \
	--offsetsToCropTo 0,0,0 \
	--dimensions 500,500,500
	"
	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
	sleep 1

