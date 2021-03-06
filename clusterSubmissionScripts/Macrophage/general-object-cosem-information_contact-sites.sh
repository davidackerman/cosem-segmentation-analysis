#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkGeneralCosemObjectInformation
N_NODES=10

INPUTN5PATH='/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/collected/Macrophage.n5'

ARGV="\
--inputPairs 'er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_mito,\
er_maskedWith_nucleus_expansion_150_to_mito,\
er_to_mito,\
er_sheetnessVolumeAveraged_ccAt1SkipSmoothing_maskedWith_nucleus_expansion_150_to_ribosomes,\
er_maskedWith_nucleus_expansion_150_to_ribosomes,\
er_to_ribosomes,\
er_to_golgi,\
er_to_plasma_membrane,\
golgi_to_MVB',
--inputN5Path '$INPUTN5PATH' \
--outputDirectory '/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/analysisResults/Macrophage/generalObjectInformation/'"

export RUNTIME="48:00"
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
