#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation
N_NODES=10

export LSF_PROJECT=cosem
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
export N_EXECUTORS_PER_NODE=2

cell=${PWD##*/}

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/HeLa2.n5'
ARGV="--inputN5DatasetName 'er_maskedWith_nucleus_expanded_maskedWith_microtubules' \
--skipContactSites \
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/HeLa2/generalObjectInformation/'"

TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
	