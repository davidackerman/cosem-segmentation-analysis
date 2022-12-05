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

mkdir -p /groups/cosem/cosem/ackermand/cosem/${cell}.n5/
while IFS=, read -r dataset labelingDataset setup
do
	trainingPath="training/finetuning/jrc_mus-liver/$setup"
        inputN5Path="/nrs/cosem/pattonw/$trainingPath"
        outputN5Path="/groups/cosem/cosem/ackermand/cosem/withFullPaths/$trainingPath"
	
	mkdir -p $outputN5Path
        ln -s $inputN5Path/volumes/${dataset}_gzipped $outputN5Path/$dataset


	ARGV="\
	--predictionN5Path $outputN5Path \
	--predictionDatasetName $dataset \
	--connectedComponentsN5Path /groups/cosem/cosem/ackermand/cosem/${cell}.n5/ \
	--connectedComponentsDatasetName $labelingDataset \
	--outputN5Path $outputN5Path \
	"

	TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
	sleep 1
	if [ ! -L /groups/cosem/cosem/ackermand/cosem/${cell}.n5/$dataset ]; then
		ln -s "$outputN5Path/${dataset}_labeledWith_${labelingDataset}" /groups/cosem/cosem/ackermand/cosem/${cell}.n5/$dataset
	fi

done < iterationsMembranes.csv
