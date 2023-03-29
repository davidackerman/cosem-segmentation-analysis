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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import static org.janelia.cosem.util.N5GenericReaderWriter.*;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
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
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;


/**
 * Threshold a prediction but label it using another segmented volume's ids.
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkLabelPredictionWithConnectedComponents {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--predictionN5Path", required = true, usage = "Path to prediction N5")
		private String predictionN5Path = null;

		@Option(name = "--predictionDatasetName", required = false, usage = "Name of prediction dataset")
		private String predictionDatasetName = null;
		
		@Option(name = "--connectedComponentsN5Path", required = false, usage = "Path to connected components dataset used for relabeling")
		private String connectedComponentsN5Path = null;
		
		@Option(name = "--connectedComponentsDatasetName", required = true, usage = "Name of connected components dataset")
		private String connectedComponentsDatasetName = null;
		
		@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
		private String outputN5Path = null;
		
		@Option(name = "--thresholdDistance", required = false, usage = "Distance for thresholding (positive inside, negative outside) (nm)")
		private double thresholdDistance = 0;
		
		public Options(final String[] args) {

			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);

				if (outputN5Path == null)
					outputN5Path = connectedComponentsN5Path;

				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getPredictionN5Path() {
			return predictionN5Path;
		}

		public String getPredictionDatasetName() {
			return predictionDatasetName;
		}

		public String getConnectedComponentsN5Path() {
			return connectedComponentsN5Path;
		}
		
		public String getConnectedComponentsDatasetName() {
			return connectedComponentsDatasetName;
		}
		
		public String getOutputN5Path() {
			return outputN5Path;
		}
		
		public double getThresholdDistance() {
			return thresholdDistance;
		}
		
	}

	
	/**
	 * Method that relabels predictions above a certain threshold with the connected component object ID they are within.
	 * 
	 * @param sc								Spark context
	 * @param predictionN5Path					N5 path to predictions
	 * @param predictionDatasetName				Name of predictions
	 * @param connectedComponentsN5Path			N5 path to connected components
	 * @param connectedComponentsDatasetName	Name of connected components
	 * @param outputN5Path						N5 path to output
	 * @param thresholdIntensityCutoff			Threshold intensity cutoff
	 * @param blockInformationList				List of block information
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends IntegerType<T> & NativeType<T>> void labelPredictionsWithConnectedComponents(
			final JavaSparkContext sc, final String predictionN5Path,final String predictionDatasetName, final String connectedComponentsN5Path,
			final String connectedComponentsDatasetName, final String outputN5Path, double thresholdIntensityCutoff, List<BlockInformation> blockInformationList) throws IOException {
					
		// Create output dataset
		final N5Reader n5Reader = N5GenericReader(connectedComponentsN5Path);
		DatasetAttributes attributes = n5Reader.getDatasetAttributes(connectedComponentsDatasetName);
		final String outputN5DatasetName = predictionDatasetName+"_labeledWith_"+connectedComponentsDatasetName;
		DataType dataType = attributes.getDataType();
		ProcessingHelper.createDatasetUsingTemplateDataset(predictionN5Path, predictionDatasetName, outputN5Path, outputN5DatasetName, dataType);
		
		// Do the labeling, parallelized over blocks
		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		rdd.foreach(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			
			final N5Reader predictionN5ReaderLocal = N5GenericReader(predictionN5Path);
			final N5Reader connectedComponentsReaderLocal = N5GenericReader(connectedComponentsN5Path);

			final double [] predictionPixelResolution = IOHelper.getResolution(predictionN5ReaderLocal, predictionDatasetName);
			final double [] connectedComponentsPixelResolution = IOHelper.getResolution(connectedComponentsReaderLocal, connectedComponentsDatasetName);

			//for now, assume connected components pixel resolution is a multiple of prediction pixel resolution
			final int scale = (int) (connectedComponentsPixelResolution[0] / predictionPixelResolution[0]);
			
			long[] predictionOffset = gridBlock[0];
			long[] predictionDimension = gridBlock[1];
			long[] connectedComponentsOffset = gridBlock[0].clone();
			long[] connectedComponentsDimension = gridBlock[1].clone();
			for(int i=0; i<3; i++) {
			    connectedComponentsOffset[i] = (int)(connectedComponentsOffset[i]*1.0/scale);
			    connectedComponentsDimension[i] = (int)(connectedComponentsDimension[i]*1.0/scale);
			}
			
			RandomAccessibleInterval<T> output = ProcessingHelper.getZerosIntegerImageRAI(predictionDimension, dataType);
			RandomAccess<T> outputRA = output.randomAccess();
			
			IntervalView<UnsignedByteType> prediction = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<UnsignedByteType>) N5Utils.open(predictionN5ReaderLocal, predictionDatasetName)
					),predictionOffset, predictionDimension);
			
					
			IntervalView<T> connectedComponents = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>) N5Utils.open(connectedComponentsReaderLocal, connectedComponentsDatasetName)
						),connectedComponentsOffset, connectedComponentsDimension);
								
			RandomAccess<UnsignedByteType> predictionRA = prediction.randomAccess();
			RandomAccess<T> connectedComponentsRA = connectedComponents.randomAccess();
			long [] previousConnectedComponentsPos = {-1,-1,-1};
			for(int x=0; x<predictionDimension[0]; x++) {
			    for(int y=0; y<predictionDimension[1]; y++) {
				for(int z=0; z<predictionDimension[2]; z++) {
				    long[] predictionPos = new long[] { x, y, z };
				    long[] connectedComponentsPos = new long[] { (long)(x*1.0/scale), (long)(y*1.0/scale), (long)(z*1.0/scale)};
				    if(connectedComponentsPos[0]!=previousConnectedComponentsPos[0] || connectedComponentsPos[1]!=previousConnectedComponentsPos[1] || connectedComponentsPos[2]!=previousConnectedComponentsPos[2]) {
					    connectedComponentsRA.setPosition(connectedComponentsPos);
					    previousConnectedComponentsPos = connectedComponentsPos;
				    }
					//    System.out.println(Arrays.toString(previousConnectedComponentsPos)+ Arrays.toString(predictionPos));

				    predictionRA.setPosition(predictionPos);
				    
				    long objectID = connectedComponentsRA.get().getIntegerLong();
				    int predictionValue = predictionRA.get().get();
				    if(objectID>0 && predictionValue>=thresholdIntensityCutoff) {
					outputRA.setPosition(predictionPos);
					outputRA.get().setInteger(objectID);
				    }
				}
			    }
			}
			
			// Write out output to temporary n5 stack
			final N5Writer n5WriterLocal = N5GenericWriter(outputN5Path);
			N5Utils.saveBlock(output, n5WriterLocal, outputN5DatasetName, gridBlock[2]);

		});
	}
	
	/**
	 * Take input arguments and label prediction with connected components
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		double thresholdIntensityCutoff = 	128 * Math.tanh(options.getThresholdDistance() / 50) + 127;

		final SparkConf conf = new SparkConf().setAppName("SparkLabelPredictionWithConnectedComponents");
		
		//Create block information list
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(options.getPredictionN5Path(), options.getPredictionDatasetName());
	
		//Run connected components
		JavaSparkContext sc = new JavaSparkContext(conf);
		labelPredictionsWithConnectedComponents(sc, options.getPredictionN5Path(), options.getPredictionDatasetName(), options.getConnectedComponentsN5Path(), options.getConnectedComponentsDatasetName(), options.getOutputN5Path(), thresholdIntensityCutoff, blockInformationList);	
		sc.close();
		
	}
}

