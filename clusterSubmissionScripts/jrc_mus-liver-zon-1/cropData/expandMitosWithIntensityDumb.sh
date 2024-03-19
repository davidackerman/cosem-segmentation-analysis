#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkExpandDataset
N_NODES=28
export N_CORES_DRIVER=4
export N_EXECUTORS_PER_NODE=1
export N_CORES_PER_EXECUTOR=20
export N_CORES_WORKER=11
# export N_OVERHEAD_CORES_PER_WORKER=8
export N_OVERHEAD_CORES_PER_WORKER=1

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk


ARGV="\
--inputN5Path '/nrs/cellmap/ackermand/cellmap/crop_jrc_mus-liver-zon-1.n5' \
--inputN5DatasetName 'mito' \
--expansionInNm 480 \
--intensityN5Path '/nrs/cellmap/ackermand/cellmap/crop_jrc_mus-liver-zon-1.n5' \
--intensityN5DatasetName 'mito_predictions_cropped' \
--outputN5DatasetSuffix '_expansion480_intensitiesMasked288dumb'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

