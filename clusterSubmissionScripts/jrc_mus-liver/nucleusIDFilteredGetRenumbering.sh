#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGetRenumbering
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}

ARGV="\
--inputN5DatasetName nucleus_volumeFiltered_renumbered_filteredIDs \
--inputN5Path /groups/cellmap/cellmap/ackermand/cellmap/jrc_mus-liver.n5/ \
--outputDirectory /groups/cellmap/cellmap/ackermand/cellmap/jrc_mus-liver.n5/
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

