#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkRenumberN5
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}

ARGV="\
--inputN5DatasetName 75_0.6_smoothed \
--inputN5Path /nrs/cellmap/ackermand/cellmap/jrc_mus-kidney.n5/watershedAndAgglomeration/mito.n5 \
--inputDirectory /nrs/cellmap/ackermand/cellmap/jrc_mus-kidney.n5/watershedAndAgglomeration/mito.n5 \
--renumberingCSV 75_0.6_smoothed
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV


