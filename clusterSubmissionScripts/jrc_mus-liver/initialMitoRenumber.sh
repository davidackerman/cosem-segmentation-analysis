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
for mito_name in {25_0.5,25_0.75,25_0.9,25_0.975,50_0.8,50_0.85,50_0.9,50_0.95,75_0.4,75_0.5,75_0.6}
do

ARGV="\
--inputN5DatasetName ${mito_name}_smoothed \
--inputN5Path /groups/cosem/cosem/ackermand/cosem/jrc_mus-liver.n5/watershedAndAgglomeration/mito.n5 \
--inputDirectory /groups/cosem/cosem/ackermand/cosem/jrc_mus-liver.n5/watershedAndAgglomeration/mito.n5 \
--renumberingCSV ${mito_name}_smoothed
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1
done

