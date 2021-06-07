#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
CLASS=org.janelia.cosem.analysis.SparkIDFilter
cell=${PWD##*/}
for cropIndex in {1,4,3,6,7,8,9,13,14,15,16,18,19,23,28}; do
    for membraneID in {2,3,6,8,10,12,14,16,18,20,22}; do
    
        ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/trainingCropMembraneThickness/${cell}.n5/crop${cropIndex}/' \
        --inputN5DatasetName 'all' \
        --outputN5DatasetSuffix '_${membraneID}' \
        --idsToKeep '${membraneID}' "

        N_NODES=1
        export N_CORES_DRIVER=1
        export RUNTIME="48:00"
        export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
        export SPARK_JANELIA_ARGS='--common_job_args "-o /dev/null"'
        TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
        sleep 1
    done
done
