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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.CorrectlyPaddedDistanceTransform;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
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
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * For a given
 * 
 * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculateThicknessHistograms {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
	private String inputN5Path = null;

	@Option(name = "--outputDirectory", required = false, usage = "Output directory")
	private String outputDirectory = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "Input N5 dataset")
	private String inputN5DatasetName = null;

	public Options(final String[] args) {
	    final CmdLineParser parser = new CmdLineParser(this);
	    try {
		parser.parseArgument(args);
		parsedSuccessfully = true;
	    } catch (final CmdLineException e) {
		parser.printUsage(System.err);
	    }
	}

	public String getInputN5Path() {
	    return inputN5Path;
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputDirectory() {
	    if (outputDirectory == null) {
		outputDirectory = inputN5Path.split(".n5")[0] + "_results";
	    }
	    return outputDirectory;
	}

    }


    /**
     * 
     * Calculate the distance transform of a dataset along its medial surface.
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return blockInformationList
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> Map<Long,Long> calculateThicknessAtMedialSurface(
	    final JavaSparkContext sc, final String n5Path, final String datasetName, 
	    final List<BlockInformation> blockInformationList) throws IOException {

	// calculate distance from medial surface
	// Parallelize analysis over blocks
	double[] pixelResolution = IOHelper.getResolution(new N5FSReader(n5Path), datasetName);
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5Path, datasetName+"_distanceTransform", DataType.FLOAT32);
	JavaRDD<Map<Long, Long>> javaRDDThicknessHistograms = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] offset = gridBlock[0];
	    final long[] dimension = gridBlock[1];
	    final N5Reader n5BlockReader = new N5FSReader(n5Path);

	    // Get correctly padded distance transform first
	    RandomAccess<T> medialSurfaceRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5Path, datasetName+"_medialSurface", offset, dimension);
	    
	    RandomAccessibleInterval<T> dataset = (RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, datasetName);
	    CorrectlyPaddedDistanceTransform cpdt = new CorrectlyPaddedDistanceTransform(dataset, offset, dimension);
	    IntervalView<FloatType> cpdtCropped = Views.offsetInterval(cpdt.correctlyPaddedDistanceTransform, cpdt.padding, dimension);
	    RandomAccess<FloatType> cpdtCroppedRA = cpdtCropped.randomAccess();
	    HashMap<Long, Long> thicknessHistogram = new HashMap<Long,Long>();
	    for (int x=0; x<dimension[0]; x++) {
		for (int y=0; y<dimension[1]; y++) {
		    for (int z=0; z<dimension[2]; z++) {
			int [] pos = {x, y, z};
			medialSurfaceRA.setPosition(pos);
			if (medialSurfaceRA.get().getIntegerLong()>0) {
			    cpdtCroppedRA.setPosition(pos);
			
			    float radiusSquared = cpdtCroppedRA.get().getRealFloat();
			    float radius = (float) Math.sqrt(radiusSquared);
			    double thickness = radius * 2;// convert later			    
			    
			    Long thicknessBin = (long) Math.min(Math.floor(thickness * pixelResolution[0]/0.5), 199);
			    thicknessHistogram.put(thicknessBin, thicknessHistogram.getOrDefault(thicknessBin, 0L) + 1L);
		   
			}
			//will remove
			cpdtCroppedRA.setPosition(pos);
			float tempradiusSquared = cpdtCroppedRA.get().getRealFloat();
			float tempradius = (float) Math.sqrt(tempradiusSquared);
			cpdtCroppedRA.get().set((float) (tempradius* pixelResolution[0]));
			//end remove

		    }
		}
	    }
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5Path);
	    N5Utils.saveBlock(cpdtCropped, n5BlockWriter, datasetName + "_distanceTransform", gridBlock[2]);
	    return thicknessHistogram;
	});
	
	Map<Long, Long> thicknessHistogramMap = javaRDDThicknessHistograms.reduce((a, b) -> {
	    for (Entry<Long, Long> entry : b.entrySet()) {
		a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0L) + entry.getValue());
	    }
	    return a;
	});

	return thicknessHistogramMap;

    }


    public static void writeData(Map<Long,Long> thicknessHistogram, String outputDirectory, String datasetName) throws IOException {
	if (!new File(outputDirectory).exists()) {
	    new File(outputDirectory).mkdirs();
	}

	FileWriter thicknessHistogramWriter = new FileWriter(
		outputDirectory + "/" + datasetName + "_thicknessHistogram.csv");
	thicknessHistogramWriter.append("Thickness Bin (nm),Count\n");

	String rowString;
	for (long thicknessBin = 0; thicknessBin < 200; thicknessBin++) {
	    //binned in half nm increments
	    String centerOfThicknessBin = Double.toString(thicknessBin*0.5 + 0.25);
	    long thicknessCount = thicknessHistogram.getOrDefault(thicknessBin, 0L);
	    rowString = centerOfThicknessBin + "," + Long.toString(thicknessCount);
	    thicknessHistogramWriter.append(rowString + "\n");
	}
	thicknessHistogramWriter.flush();
	thicknessHistogramWriter.close();
    }


    public static void setupSparkAndCalculateThicknessHistograms(String inputN5Path, String inputN5DatasetName, String outputDirectory)
	    throws IOException {

	final SparkConf conf = new SparkConf().setAppName("SparkCalculateThicknessHistograms");

	for (String organelle : inputN5DatasetName.split(",")) {
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    organelle);

	    JavaSparkContext sc = new JavaSparkContext(conf);
	    Map<Long, Long> thicknessHistogram = calculateThicknessAtMedialSurface(sc, inputN5Path, organelle, blockInformationList);
	    writeData(thicknessHistogram, outputDirectory, inputN5DatasetName);
	    sc.close();
	}
    }

    /**
     * Take input args and perform the calculation
     * 
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {
	final Options options = new Options(args);

	if (!options.parsedSuccessfully)
	    return;

	String inputN5Path = options.getInputN5Path();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String outputDirectory = options.getOutputDirectory();

	setupSparkAndCalculateThicknessHistograms(inputN5Path, inputN5DatasetName, outputDirectory);

    }

}
