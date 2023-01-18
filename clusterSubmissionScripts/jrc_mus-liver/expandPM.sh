#!/bin/bash

#!/bin/bash

OWN_DIR='/groups/scicompsoft/home/ackermand/Programming/cosem-segmentation-analysis'
JAR=$OWN_DIR/target/cosem-segmentation-analysis-0.0.1-SNAPSHOT.jar

FLINTSTONE=/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh
CLASS=org.janelia.cosem.analysis.SparkExpandDataset
N_NODES=15

export LSF_PROJECT=cellmap
export N_CORES_DRIVER=1
export RUNTIME="48:00"
export JAVA_HOME="/usr/lib/jvm/java-1.8.0"


ARGV="--inputN5DatasetName 'plasma_membrane' \
--inputN5Path '/nrs/cellmap/ackermand/cellmap/jrc_mus-liver.n5' \
--expansionInNm 2000 \
--outputN5DatasetSuffix '_expansion_2000' \
--useFixedValue"

export MEMORY_PER_NODE=500
TERMINATE=1 $FLINTSTONE $N_NODES $JAR $CLASS $ARGV
