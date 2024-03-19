#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGetRenumbering
N_NODES=200
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
--inputN5Path '/nrs/cellmap/zouinkhim/predictions/jrc_mus-liver-zon-1/jrc_mus-liver-zon-1_postprocessed.n5/mito'  \
--inputN5DatasetName 'postprocessed_mito' \
--outputDirectory '/nrs/cellmap/ackermand/cellmap/renumbering/renumberingForMarwan/jrc_mus-liver-zon-1'
"

export N_CORES_DRIVER=1
export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
