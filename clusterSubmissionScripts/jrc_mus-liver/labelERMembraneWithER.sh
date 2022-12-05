#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkLabelPredictionWithConnectedComponents
N_NODES=10

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

cell=${PWD##*/}

ARGV="\
--predictionN5Path /nrs/cellmap/pattonw/predictions/jrc_mus-liver/${cell}.n5/evaluations-best/full
--connectedComponentsN5Path /groups/cellmap/cellmap/ackermand/cellmap/${cell}.n5 \
--predictionDatasetName er_membrane
--connectedComponentsDatasetName er_volumeFiltered_renumbered
"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV

