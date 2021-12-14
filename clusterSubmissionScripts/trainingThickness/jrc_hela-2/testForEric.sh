#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
export LSF_PROJECT=cosem
CLASS=org.janelia.cosem.analysis.SparkTopologicalThinning
    
ARGV="--inputN5Path '/groups/cosem/cosem/ackermand/trainingCropMembraneThickness/jrc_hela-2.n5/crop23/' \
--outputN5Path '/groups/cosem/cosem/ackermand/testForEric.n5/' \
--inputN5DatasetName 'all_3' \
--doMedialSurface"

N_NODES=1
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
#export SPARK_JANELIA_ARGS='--common_job_args "-o /dev/null"'
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

