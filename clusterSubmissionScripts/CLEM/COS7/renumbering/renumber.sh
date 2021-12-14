#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkRenumberN5
N_NODES=10

export LSF_PROJECT=cosem
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
export N_EXECUTORS_PER_NODE=2

N_NODES=10

currentDirectory="$(dirname "${PWD}")"
cell=${currentDirectory##*/}


ARGV="\
--inputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/COS7_Cell11.n5'  \
--inputN5DatasetName 'er_cc,mito_cc_filled' \
--outputN5Path '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/renumbered.n5'  \
--inputDirectory '/groups/cosem/cosem/ackermand/CLEM/COS7/imageData/renumbering' \
--renumberingCSV 'er_cc,mito_cc_filled' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &

