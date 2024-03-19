#!/bin/bash
  
OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkFillHolesInConnectedComponents
N_NODES=30

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

INPUTN5PATH="/nrs/cellmap/ackermand/cellmap/withFullPaths/jrc_mus-liver-zon-1/jrc_mus-liver-zon-1.n5/predictions/2023-04-10/nucleus/best/"

ARGV="\
--inputN5DatasetName 'nucleus_cc' \
--skipVolumeFilter \
--outputN5DatasetSuffix '_filled' \
--inputN5Path '$INPUTN5PATH' \
"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

