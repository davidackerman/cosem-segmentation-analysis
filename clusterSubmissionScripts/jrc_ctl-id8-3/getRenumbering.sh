#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGetRenumbering
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
#export N_EXECUTORS_PER_NODE=2

cell=${PWD##*/}


	ARGV="\
	--inputN5DatasetName cell_seg \
	--outputDirectory /groups/cellmap/cellmap/ackermand/cosem/tmp/ \
	--inputN5Path /nrs/cellmap/bennettd/s3/janelia-cosem-datasets/jrc_ctl-id8-3/jrc_ctl-id8-3.n5/labels/ariadne/ \
	"
	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
	sleep 1
