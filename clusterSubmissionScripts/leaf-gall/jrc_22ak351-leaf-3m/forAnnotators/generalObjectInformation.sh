#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkGeneralCosemObjectInformation
N_NODES=100
# cluster currently full so decreasing the amount
#export N_CORES_DRIVER=30
#export N_EXECUTORS_PER_NODE=15
export N_CORES_DRIVER=4
export N_EXECUTORS_PER_NODE=1
export N_CORES_PER_EXECUTOR=10
# export N_OVERHEAD_CORES_PER_WORKER=8
export N_OVERHEAD_CORES_PER_WORKER=1

export LSF_PROJECT=cellmap
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"

for dataset in {'tp_gt','tp_pred','fn_gt','fp_pred'}; do
    ARGV="\
    --inputN5Path '/nrs/cellmap/ackermand/forAnnotators/leaf-gall/jrc_22ak351-leaf-3m_2023-12-06.n5' \
    --outputDirectory '/nrs/cellmap/ackermand/forAnnotators/leaf-gall/analysisResults/jrc_22ak351-leaf-3m_2023-12-06' \
    --inputN5DatasetName '$dataset' \
    --skipContactSites "

    TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
    sleep 1;
done
