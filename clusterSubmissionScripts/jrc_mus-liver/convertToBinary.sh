#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConvertToBinary
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}
#for organelle in {er,nucleus,plasma_membrane,mito,er_membrane,mito_membrane}; do
for organelle in {er_membrane,mito_membrane}; do

ARGV="\
--inputN5DatasetName $organelle \
--inputN5Path /groups/cosem/cosem/ackermand/cosem/jrc_mus-liver.n5 \
--outputN5Path /groups/cosem/cosem/ackermand/cosem/jrc_mus-liver.n5/binarized.n5 \
--label 255
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1
done

