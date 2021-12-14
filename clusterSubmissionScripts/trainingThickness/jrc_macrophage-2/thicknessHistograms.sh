#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis' #`dirname "${BASH_SOURCE[0]}"`
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkCalculateThicknessHistograms

export LSF_PROJECT=cosem
N_NODES=1
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
export SPARK_JANELIA_ARGS='--common_job_args "-o /dev/null"'

cell=${PWD##*/}

while IFS=, read -r cropIndex
do

    inputPath="/groups/cosem/cosem/ackermand/trainingCropMembraneThickness/${cell}.n5/crop${cropIndex}/"

    for membraneID in {2,3,6,8,10,12,14,16,18,20,22,16_18_20_22}; do
        ARGV="--inputN5Path '${inputPath}' \
        --outputDirectory '/groups/cosem/cosem/ackermand/trainingCropMembraneThickness/analysis/${cell}/crop${cropIndex}/' \
        --inputN5DatasetName 'all_${membraneID}' \
        --pixelResolution '2.0,2.0,1.68' "

        TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
        sleep 2
    done

done < crops.csv

