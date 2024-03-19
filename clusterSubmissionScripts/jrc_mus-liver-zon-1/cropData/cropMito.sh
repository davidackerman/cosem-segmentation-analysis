#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkCrop
N_NODES=20
export N_CORES_DRIVER=4
export N_EXECUTORS_PER_NODE=1
export N_CORES_PER_EXECUTOR=10
# export N_OVERHEAD_CORES_PER_WORKER=8
export N_OVERHEAD_CORES_PER_WORKER=1

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk

ARGV="\
--inputN5Path '/nrs/cellmap/zouinkhim/predictions/jrc_mus-liver-zon-1/jrc_mus-liver-zon-1_postprocessed.n5/mito' \
--outputN5Path '/nrs/cellmap/ackermand/cellmap/crop_jrc_mus-liver-zon-1.n5' \
--outputN5DatasetSuffix '_cropped'
--inputN5DatasetName 'postprocessed_mito' \
--offsetsToCropTo 61952,35072,120000 \
--dimensions 6000,6000,6000 "

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1
