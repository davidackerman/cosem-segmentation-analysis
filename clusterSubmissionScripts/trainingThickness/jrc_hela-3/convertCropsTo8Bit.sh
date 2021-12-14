#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar
FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkConvertCropsTo8Bit

export LSF_PROJECT=cosem
N_NODES=1
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
export N_EXECUTORS_PER_NODE=2

cell=${PWD##*/}
while IFS=, read -r cropIndex
do
    ARGV="--inputN5Path '/groups/cosem/cosem/data/${cell}/${cell}.n5/volumes/groundtruth/0003/crop${cropIndex}/labels/' \
    --inputN5DatasetName 'all' \
    --outputN5Path '/groups/cosem/cosem/ackermand/trainingCropMembraneThickness/${cell}.n5/crop${cropIndex}/' "

    TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
    sleep 2
done < crops.csv
