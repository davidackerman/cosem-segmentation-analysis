/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.cosem.analysis;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DataType;
import static org.janelia.cosem.util.N5GenericReaderWriter.*;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;

/**
 * Convert crops to 8 bit
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkConvertToBinary {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = true, usage = "N5 dataset, e.g. /mito")
	private String inputN5DatasetName = null;
	
	@Option(name = "--label", required = false, usage = "label")
	private int label = 1;

	public Options(final String[] args) {

	    final CmdLineParser parser = new CmdLineParser(this);
	    try {
		parser.parseArgument(args);

		if (outputN5Path == null)
		    outputN5Path = inputN5Path;

		parsedSuccessfully = true;
	    } catch (final CmdLineException e) {
		System.err.println(e.getMessage());
		parser.printUsage(System.err);
	    }
	}

	public String getInputN5Path() {
	    return inputN5Path;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}
	
	public int getLabel() {
	    return label;
	}

    }

    
    
    public static final <T extends IntegerType<T> & NativeType<T>> void relabel(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,final int label,
	    final List<BlockInformation> blockInformationList)
	    throws IOException {

	DataType dataType = DataType.UINT8;
	String tempDatasetName = datasetName;
	if(n5OutputPath==n5Path) {
	    tempDatasetName+="_relabeled";
	}
	final String outputDatasetName = tempDatasetName;
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, outputDatasetName, dataType);

	/*
	 * grid block size for parallelization to minimize double loading of blocks
	 */
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];
	    long[] dimension = gridBlock[1];

	    Cursor<T> sourceCursor = ProcessingHelper.getOffsetIntervalExtendZeroC(n5Path, datasetName,
		    offset, dimension);
	    RandomAccessibleInterval<T> output = ProcessingHelper.getZerosIntegerImageRAI(dimension, dataType);
	    RandomAccess<T> outputRA = output.randomAccess();

	    long[] pos;
	    while (sourceCursor.hasNext()) {
		sourceCursor.next();
		long objectID = sourceCursor.get().getIntegerLong();
		if (objectID > 0) {
		    pos = new long[] { sourceCursor.getLongPosition(0), sourceCursor.getLongPosition(1),
			    sourceCursor.getLongPosition(2) };
		    outputRA.setPosition(pos);
		    outputRA.get().setInteger(label);
		}
	    }
	    final N5Writer n5BlockWriter = N5GenericWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
	});
	
    }

    
    public static void setupSparkAndRelabelN5(String inputN5DatasetName, String inputN5Path,
	    String outputN5Path, int label) throws Exception {
	final SparkConf conf = new SparkConf().setAppName("SparkConvertToBinary");	

	// Get all organelles
	String[] organelles = { "" };
	if (inputN5DatasetName != null) {
	    organelles = inputN5DatasetName.split(",");
	} else {
	    File file = new File(inputN5Path);
	    organelles = file.list(new FilenameFilter() {
		@Override
		public boolean accept(File current, String name) {
		    return new File(current, name).isDirectory();
		}
	    });
	}
	
	for(int i=0; i < organelles.length; i++) {

	    // Create block information list
	    List<BlockInformation> blockInformationList = BlockInformation
		    .buildBlockInformationList(inputN5Path, organelles[i]);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    relabel(sc, inputN5Path, organelles[i], outputN5Path, label, blockInformationList);
	
	    sc.close();
	}
    }
    
    public static final void main(final String... args) throws Exception {

	final Options options = new Options(args);

	if (!options.parsedSuccessfully)
	    return;
	setupSparkAndRelabelN5(options.getInputN5DatasetName(),
	options.getInputN5Path(),
	options.getOutputN5Path(),
	options.getLabel());
	

    }

    
}
