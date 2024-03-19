#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkExpandDataset
N_NODES=20
export N_CORES_DRIVER=48
export N_EXECUTORS_PER_NODE=20
export N_CORES_PER_EXECUTOR=2
export N_OVERHEAD_CORES_PER_WORKER=8

export LSF_PROJECT=cellmap
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}


ARGV="\
--inputN5Path '/nrs/cellmap/ackermand/cellmap/jrc_mus-kidney.n5' \
--inputN5DatasetName 'nucleus' \
--expansionInNm 80 \
--useOnlySurfaceVoxels \
--outputN5DatasetSuffix '_surfaceVoxels_expansion80'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
sleep 1

