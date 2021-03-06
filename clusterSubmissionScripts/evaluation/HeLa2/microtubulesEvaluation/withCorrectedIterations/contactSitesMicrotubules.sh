#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/hot-knife' #`dirname "${BASH_SOURCE[0]}"`
ABS_DIR=`readlink -f "$OWN_DIR"`

FLINTSTONE=$OWN_DIR/flintstone/flintstone-lsd.sh
JAR=$OWN_DIR/target/hot-knife-0.0.4-SNAPSHOT.jar
CLASS=org.janelia.saalfeldlab.hotknife.SparkContactSites
N_NODES=3

export RUNTIME="48:00"
BASENAME=/groups/cosem/cosem/ackermand/paperResultsWithFullPaths/evaluation/HeLa2/microtubuleEvaluation/

for i in {validation,refinedPredictions}
do

for j in {crop1,crop2,crop3,crop4}
do

INPUTPAIRS=vesicle_maskedWith_microtubules_to_microtubules

	ARGV="\
	--inputPairs '$INPUTPAIRS' \
	--contactDistance 20 \
	--inputN5Path '$BASENAME/$i/${j}.n5' \
	--minimumVolumeCutoff 0 \
	"
	
	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV &
	sleep 2 
done

done
