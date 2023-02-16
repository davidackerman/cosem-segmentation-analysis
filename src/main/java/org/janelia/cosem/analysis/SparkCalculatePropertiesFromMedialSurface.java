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
import java.util.ArrayList;
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
import org.janelia.cosem.util.CorrectlyPaddedGeodesicDistanceTransform;
import org.janelia.cosem.util.GeodesicDistanceTransform3DFloat;
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

import ij.ImageJ;
import ij.ImageStack;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import inra.ijpb.binary.ChamferWeights3D;

/**
 * For a given
 * 
 * TODO: start bins at 1 so that wont run into issues with 0 being mixed in with
 * background
 * 
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkCalculatePropertiesFromMedialSurface {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "Input N5 path")
	private String inputN5Path = null;

	@Option(name = "--outputDirectory", required = false, usage = "Output directory")
	private String outputDirectory = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "Onput N5 dataset")
	private String inputN5DatasetName = null;

	@Option(name = "--outputN5Path", required = false, usage = "Output N5 path")
	private String outputN5Path = null;

	@Option(name = "--calculateAreaAndVolumeFromExistingDataset", required = false, usage = "Use existing volume averaged sheetness")
	private boolean calculateAreaAndVolumeFromExistingDataset = false;

	public Options(final String[] args) {
	    final CmdLineParser parser = new CmdLineParser(this);
	    try {
		parser.parseArgument(args);
		if (outputN5Path == null)
		    outputN5Path = inputN5Path;
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
		outputDirectory = outputN5Path.split(".n5")[0] + "_results";
	    }
	    return outputDirectory;
	}

	public String getOutputN5Path() {
	    return outputN5Path;
	}

	public boolean getCalculateAreaAndVolumeFromExistingDataset() {
	    return calculateAreaAndVolumeFromExistingDataset;
	}

    }

    /**
     * Class to contain histograms as maps for sheetness, thickness, surface area
     * and volume
     */
    public static class HistogramMaps implements Serializable {
	private static final long serialVersionUID = 1L;
	public Map<List<Integer>, Long> sheetnessAndThicknessHistogram;
	public Map<Integer, Double> sheetnessAndSurfaceAreaHistogram;
	public Map<Integer, Double> sheetnessAndVolumeHistogram;

	public HistogramMaps(Map<List<Integer>, Long> sheetnessAndThicknessHistogram,
		Map<Integer, Double> sheetnessAndSurfaceAreaHistogram,
		Map<Integer, Double> sheetnessAndVolumeHistogram) {
	    this.sheetnessAndThicknessHistogram = sheetnessAndThicknessHistogram;
	    this.sheetnessAndSurfaceAreaHistogram = sheetnessAndSurfaceAreaHistogram;
	    this.sheetnessAndVolumeHistogram = sheetnessAndVolumeHistogram;
	}

	public void merge(HistogramMaps newHistogramMaps) {
	    // merge holeIDtoObjectIDMap
	    for (Entry<List<Integer>, Long> entry : newHistogramMaps.sheetnessAndThicknessHistogram.entrySet())
		sheetnessAndThicknessHistogram.put(entry.getKey(),
			sheetnessAndThicknessHistogram.getOrDefault(entry.getKey(), 0L) + entry.getValue());

	    // merge holeIDtoVolumeMap
	    for (Entry<Integer, Double> entry : newHistogramMaps.sheetnessAndSurfaceAreaHistogram.entrySet())
		sheetnessAndSurfaceAreaHistogram.put(entry.getKey(),
			sheetnessAndSurfaceAreaHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue());

	    // merge objectIDtoVolumeMap
	    for (Entry<Integer, Double> entry : newHistogramMaps.sheetnessAndVolumeHistogram.entrySet())
		sheetnessAndVolumeHistogram.put(entry.getKey(),
			sheetnessAndVolumeHistogram.getOrDefault(entry.getKey(), 0.0) + entry.getValue());

	}

    }

    public static class PlanarityCalculator {

	public class VoxelProperties {
	    public List<List<Long>> nearestNeighbors;
	    public float planaritySum=0;
	    public int planarityCount=0;
	    public float planarity=0;

	    VoxelProperties(float planaritySum, int planarityCount){
		this.planaritySum = planaritySum;
		this.planarityCount = planarityCount;
		this.planarity = planaritySum/planarityCount;
	    }

	}

	private HashMap<List<Long>, Float> deltaToWeightsMap = new HashMap<List<Long>, Float>();
	private HashMap<List<Long>, VoxelProperties> voxelToPropertiesMap = new HashMap<List<Long>, VoxelProperties>();
	private RandomAccess<FloatType> geodesicDistancesRA;
	private RandomAccess<FloatType> planarityRA;


	PlanarityCalculator(RandomAccess<FloatType> geodesicDistancesRA, RandomAccess<FloatType> planarityRA, float[] weights) {
	    this.geodesicDistancesRA = geodesicDistancesRA;
	    this.planarityRA = planarityRA;

	    for (long deltaX = -1; deltaX <= 1; deltaX++) {
		for (long deltaY = -1; deltaY <= 1; deltaY++) {
		    for (long deltaZ = -1; deltaZ <= 1; deltaZ++) {
			Long[] delta = new Long[] { deltaX, deltaY, deltaZ };
			long distance = delta[0] * delta[0] + delta[1] * delta[1] + delta[2] * delta[2];
			Float geodesicDistance = (float) 0.0;
			if (distance == 1) {
			    geodesicDistance = weights[0];
			} else if (distance == 2) {
			    geodesicDistance = weights[1];
			} else if (distance == 3) {
			    geodesicDistance = weights[2];
			}
			deltaToWeightsMap.put(Arrays.asList(delta), geodesicDistance);
		    }
		}
	    }
	}

	private List<List<Long>> getNearestNeighbors(long[] position) {
	    geodesicDistancesRA.setPosition(position);
	    List<List<Long>> nearestNeighbors = new ArrayList<List<Long>>();
	    float centerGeodesicDistance = geodesicDistancesRA.get().get();
	    if (centerGeodesicDistance == 0) {
		//Then is on medial surface and so takes the value of the medial surface
		return nearestNeighbors;
	    }

	    for (long deltaX = -1; deltaX <= 1; deltaX++) {
		for (long deltaY = -1; deltaY <= 1; deltaY++) {
		    for (long deltaZ = -1; deltaZ <= 1; deltaZ++) {
			if (!(deltaX == 0 && deltaY == 0 && deltaZ == 0)) {
			    long[] neighborPosition = new long[] { position[0] + deltaX, position[1] + deltaY,
				    position[2] + deltaZ };
			    geodesicDistancesRA.setPosition(neighborPosition);
			    float neighborGeodesicDistance = geodesicDistancesRA.get().get();
			    List<Long> delta = Arrays.asList( deltaX, deltaY, deltaZ );
			    if (centerGeodesicDistance
				    - this.deltaToWeightsMap.get(delta) == neighborGeodesicDistance) {
				// then this is the path to take
				nearestNeighbors.add(Arrays.asList( position[0] + deltaX, position[1] + deltaY,
					position[2] + deltaZ ));
			    }

			}
		    }
		}
	    }
	    return nearestNeighbors;
	}

	private void updateVoxelProperties(long [] position) {
	    float planaritySum = 0;
	    int planarityCount = 0;

	    List<List<Long>> nearestNeighbors = this.getNearestNeighbors(position);

	    if(nearestNeighbors.isEmpty()) {
		//Then is on medial surface
		this.planarityRA.setPosition(position);
		planaritySum = planarityRA.get().get();	
		planarityCount = 1;
	    }
	    else {
		for(List<Long> nearestNeighbor : nearestNeighbors) {
		    if(! voxelToPropertiesMap.containsKey(nearestNeighbor)){
			updateVoxelProperties(new long [] {nearestNeighbor.get(0), nearestNeighbor.get(1), nearestNeighbor.get(2)});
		    }
		    VoxelProperties voxelProperties = voxelToPropertiesMap.get(nearestNeighbor);
		    planaritySum += voxelProperties.planaritySum;
		    planarityCount+=voxelProperties.planarityCount;
		}
	    }

	    VoxelProperties voxelProperties = new VoxelProperties(planaritySum,planarityCount);
	    voxelToPropertiesMap.put(Arrays.asList(position[0],position[1],position[2]),voxelProperties);
	}

	public float getPlanarity(long [] position) {
	    VoxelProperties voxelProperties = this.voxelToPropertiesMap.getOrDefault(Arrays.asList(position[0],position[1],position[2]),null);
	    if(voxelProperties==null) {
		this.updateVoxelProperties(position);
		voxelProperties = this.voxelToPropertiesMap.get(Arrays.asList(position[0],position[1],position[2]));
	    }
	    this.geodesicDistancesRA.setPosition(position);
	    if(this.geodesicDistancesRA.get().get() ==32) {
		//	System.out.println(this.getNearestNeighbors(position).size());
	    }
	    //System.out.println(this.geodesicDistancesRA.get().get() + " "+voxelProperties.planarityCount);
	    return voxelProperties.planarity;
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
    public static final <T extends IntegerType<T> & NativeType<T>> HistogramMaps calculatePropertiesFromMedialSurface(final JavaSparkContext sc,
	    final String n5Path, final String datasetName, final String n5OutputPath,
	    final List<BlockInformation> blockInformationList) throws IOException {

	String outputDatasetName = datasetName + "_medialSurfaceDistanceTransform";
	// General information
	final int[] blockSize = new N5FSReader(n5Path).getDatasetAttributes(datasetName).getBlockSize();
	final long[] offsets = IOHelper.getOffset(new N5FSReader(n5Path), datasetName);
	final long[] dimensions = new N5FSReader(n5Path).getDatasetAttributes(datasetName).getDimensions();

	double[] pixelResolution = IOHelper.getResolution(new N5FSReader(n5Path), datasetName);
	double voxelVolume = pixelResolution[0] * pixelResolution[1] * pixelResolution[2];
	double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, datasetName+"_planarity",
		DataType.UINT8);
	// calculate distance from medial surface
	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	//JavaRDD<BlockInformation> blockInformationListWithPadding = 
	JavaRDD<HistogramMaps> javaRDDHistogramMaps = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] offset = gridBlock[0];
	    final long[] dimension = gridBlock[1];
	    final N5Reader n5BlockReader = new N5FSReader(n5Path);

	    // Get correctly padded distance transform first
	    RandomAccessibleInterval<T> segmentation = (RandomAccessibleInterval<T>) N5Utils
		    .open(n5BlockReader, datasetName);
	    RandomAccess<T> segmentationRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(
	        n5Path, datasetName, new long [] {offset[0]-1,offset[1]-1,offset[2]-1}, new long [] {dimension[0]+2,dimension[1]+2,dimension[2]+2});
	    
	    RandomAccessibleInterval<T> medialSurface = (RandomAccessibleInterval<T>) N5Utils
		    .open(n5BlockReader, datasetName+"_medialSurface");
	    RandomAccessibleInterval<T> medialSurfaceRAI = ProcessingHelper.getOffsetIntervalExtendZeroRAI(
		    n5Path, datasetName + "_medialSurface", offset, dimension);

	    // medial surface distance transform
	    CorrectlyPaddedDistanceTransform cpdt = new CorrectlyPaddedDistanceTransform(segmentation, offset, dimension);
	    RandomAccessibleInterval<FloatType> dtRAI = Views.offsetInterval(cpdt.correctlyPaddedDistanceTransform, cpdt.padding, dimension);
	    LoopBuilder.setImages(dtRAI, medialSurfaceRAI).forEachPixel((dtra,msra) -> { if(msra.getIntegerLong()==0) dtra.set(Float.NaN);  });
	    RandomAccess<FloatType> dtRA = dtRAI.randomAccess();

	    // planarity of whole object
	    CorrectlyPaddedGeodesicDistanceTransform cpgdt = new CorrectlyPaddedGeodesicDistanceTransform(medialSurface, segmentation, offset, dimension, false);
	    RandomAccess<FloatType> planarityRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5Path,
		    datasetName + "_sheetness", cpgdt.paddedOffset, cpgdt.paddedDimension);
	    IntervalView<UnsignedByteType> outputPlanarity = Views.offsetInterval(ArrayImgs.unsignedBytes(dimension),new long[]{0,0,0}, dimension);
	    RandomAccess<UnsignedByteType> outputPlanarityRA = outputPlanarity.randomAccess();
	    RandomAccess<FloatType> cpgdtRA = cpgdt.correctlyPaddedGeodesicDistanceTransform.randomAccess();
	    PlanarityCalculator planarityCalculator = new PlanarityCalculator(cpgdt.correctlyPaddedGeodesicDistanceTransform.randomAccess(), planarityRA, cpgdt.weights);

	    // set up maps
	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();
	    Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
	    Map<Integer, Double> sheetnessAndVolumeHistogram = new HashMap<Integer, Double>();

	    for (long x = cpgdt.padding[0]; x < cpgdt.paddedDimension[0] - cpgdt.padding[0]; x++) {
		for (long y = cpgdt.padding[1]; y < cpgdt.paddedDimension[1] - cpgdt.padding[1]; y++) {
		    for (long z = cpgdt.padding[2]; z < cpgdt.paddedDimension[2] - cpgdt.padding[2]; z++) {
			long [] position = new long[] {x,y,z};
			cpgdtRA.setPosition(position);
			if( cpgdtRA.get().get() < Float.POSITIVE_INFINITY) {
			    //TODO: simple test
			    float planarity = planarityCalculator.getPlanarity(position);
			    int planarityBin = (int) Math.ceil(planarity * 254) + 1;
			    outputPlanarityRA.setPosition(new long[] {x-cpgdt.padding[0], y-cpgdt.padding[1], z-cpgdt.padding[2]});
			    outputPlanarityRA.get().set(planarityBin);
			    sheetnessAndVolumeHistogram.put(planarityBin,
				    sheetnessAndVolumeHistogram.getOrDefault(planarityBin, 0.0) + voxelVolume);

			    segmentationRA.setPosition(new long[] {x-cpgdt.padding[0]+1, y-cpgdt.padding[1]+1, z-cpgdt.padding[2]+1});
			    int faces = ProcessingHelper.getSurfaceAreaContributionOfVoxelInFaces(segmentationRA,
				    new long [] {offset[0]-1,offset[1]-1,offset[2]-1}, dimensions, true);
			    if (faces > 0) {
				sheetnessAndSurfaceAreaHistogram.put(planarityBin,
					sheetnessAndSurfaceAreaHistogram.getOrDefault(planarityBin, 0.0)
					+ faces * voxelFaceArea);
			    }

			    dtRA.setPosition(new long[] {x-cpgdt.padding[0], y-cpgdt.padding[1], z-cpgdt.padding[2]});
			    float halfThickness = dtRA.get().get();
			    if ( Float.isNaN(halfThickness) ) {
				double thickness = halfThickness * 2;// convert later
				// bin thickness in 8 nm bins
				int thicknessBin = (int) Math.min(Math.floor(thickness * pixelResolution[0] / 8), 99);
				List<Integer> histogramBinList = Arrays.asList(planarityBin, thicknessBin);
				sheetnessAndThicknessHistogram.put(histogramBinList,
					sheetnessAndThicknessHistogram.getOrDefault(histogramBinList, 0L) + 1L);
			    }


			}

			//randomAcess().setPosition(new long[] {x,y,z});
		    }
		}
	    }

	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(outputPlanarity, n5BlockWriter, datasetName+"_planarity", gridBlock[2]);
	    return new HistogramMaps(sheetnessAndThicknessHistogram, sheetnessAndSurfaceAreaHistogram,
		    sheetnessAndVolumeHistogram);
	});

	HistogramMaps histogramMaps = javaRDDHistogramMaps.reduce((a, b) -> {
	    a.merge(b);
	    return a;
	});

	/*final long[][] gridBlock = blockInformation.gridBlock;
	final long[] dimension = gridBlock[1];
	final long[] paddedOffset = blockInformation.getPaddedOffset(1);
	final long[] paddedDimension = blockInformation.getPaddedDimension(1);

	final N5Reader n5BlockReader = new N5FSReader(n5OutputPath);

	// Get corresponding medial surface and sheetness
	RandomAccess<UnsignedByteType> sheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5OutputPath,
		datasetName, paddedOffset, paddedDimension);

	Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();
	Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
	Map<Integer, Double> sheetnessAndVolumeHistogram = new HashMap<Integer, Double>();

	for (long x = cpgdt.padding[0] - 1; x < cpgdt.padding[0]+dimension[0]; x++) {
	    for (long y = cpgdt.padding[1] - 1; y < cpgdt.padding[1] + 1; y++) {
		for (long z = cpgdt.padding[2] - 1; z < cpgdt.padding[2] + 1; z++) {
		    long[] pos = new long[] { x, y, z };
		    sheetnessRA.setPosition(pos);
		    int sheetnessMeasureBin = sheetnessRA.get().get();
		    if (sheetnessMeasureBin > 0) {
			sheetnessAndVolumeHistogram.put(sheetnessMeasureBin,
				sheetnessAndVolumeHistogram.getOrDefault(sheetnessMeasureBin, 0.0) + voxelVolume);
			int faces = ProcessingHelper.getSurfaceAreaContributionOfVoxelInFaces(sheetnessRA,
				paddedOffset, dimensions);
			if (faces > 0) {
			    sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin,
				    sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin, 0.0)
				    + faces * voxelFaceArea);
			}
		    }
		}
	    }
	}
	return new HistogramMaps(sheetnessAndThicknessHistogram, sheetnessAndSurfaceAreaHistogram,
		sheetnessAndVolumeHistogram);
    });

    HistogramMaps histogramMaps = javaRDDHistogramMaps.reduce((a, b) -> {
	a.merge(b);
	return a;
    });
	 */
	return histogramMaps;
    }

    // Write out volume averaged sheetness

    //return blockInformation;


    //blockInformation.paddingForMedialSurface = 0;
    //TODO: don't use regular distance transform, use this one
    /*for (long x = cpgdt.padding[0]; x < cpgdt.paddedDimension[0] - cpgdt.padding[0]; x++) {
			for (long y = cpgdt.padding[1]; y < cpgdt.paddedDimension[1] - cpgdt.padding[1]; y++) {
			    for (long z = cpgdt.padding[2]; z < cpgdt.paddedDimension[2] - cpgdt.padding[2]; z++) {
				long[] pos = new long[] { x, y, z };
				medialSurfaceRandomAccess.setPosition(pos);
				if (medialSurfaceRandomAccess.get().getIntegerLong() > 0) {
				    distanceTransformRA.setPosition(pos);
				    //float dist = (float) Math.sqrt(distanceTransformRA.get().get());
				    float dist = distanceTransformRA.get().get();
				    for (int i = 0; i < 3; i++) {
					float negativePadding = dist - (pos[i] - cpgdt.padding[i]);

					long positiveBorder = cpgdt.paddedDimension[i] - cpgdt.padding[i] - 1;
					float positivePadding = dist - (positiveBorder - pos[i]);

					int maxPadding = (int) Math.ceil(Math.max(negativePadding, positivePadding)) + 1;

					if (maxPadding > blockInformation.paddingForMedialSurface) {
					    blockInformation.paddingForMedialSurface = maxPadding;
					}
				    }

				}
			    }
			}
		    }*/
    /*IntervalView<FloatType> output = Views.offsetInterval(cpgdt.correctlyPaddedGeodesicDistanceTransform, cpgdt.padding,
			    dimension);*/
    /*float[] floatWeights = ChamferWeights3D.BORGEFORS.getFloatWeights();
			 boolean normalize = true;
			 DistanceTransform3D dt = new DistanceTransform3DFloat(floatWeights, normalize);
			 ImageStack result = dt.distanceMap(inputStack);*/
    /*List<BlockInformation> updatedBlockInformationList = blockInformationListWithPadding.collect();

	int[] maxPaddingForBlock = new int[updatedBlockInformationList.size()];
	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    maxPaddingForBlock[i] = updatedBlockInformationList.get(i).paddingForMedialSurface;
	}

	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    BlockInformation blockInformationOne = updatedBlockInformationList.get(i);
	    int paddingForMedialSurfaceOne = blockInformationOne.paddingForMedialSurface;
	    int numberOfBlocksToExpandBy = (int) Math.ceil(paddingForMedialSurfaceOne * 1.0 / blockSize[0]) + 1;

	    long[] gridLocationOne = blockInformationOne.gridBlock[2];
	    for (int j = 0; j < updatedBlockInformationList.size(); j++) {

		BlockInformation blockInformationTwo = updatedBlockInformationList.get(j);
		long[] gridLocationTwo = blockInformationTwo.gridBlock[2];
		long dx = gridLocationOne[0] - gridLocationTwo[0];
		long dy = gridLocationOne[1] - gridLocationTwo[1];
		long dz = gridLocationOne[2] - gridLocationTwo[2];

		double gridDistance = Math.sqrt(dx * dx + dy * dy + dz * dz);
		if (gridDistance < numberOfBlocksToExpandBy) {
		    if (paddingForMedialSurfaceOne > maxPaddingForBlock[j]) {
			// in case it goes beyond the neighboring, box, need to expand it by the box
			// dimension
			maxPaddingForBlock[j] = paddingForMedialSurfaceOne;
		    }
		}
	    }

	}
	for (int i = 0; i < updatedBlockInformationList.size(); i++) {
	    updatedBlockInformationList.get(i).paddingForMedialSurface = maxPaddingForBlock[i];
	    // System.out.println(updatedBlockInformationList.get(i).paddingForMedialSurface);
	}

	return updatedBlockInformationList;*/


    /**
     * 
     * Create a sheetness volume from the medial surface by expanding the medial
     * surface with a sphere centered at each voxel in the medial surface, with a
     * radius equal to the distance transform at that voxel. The value of voxels
     * within the sphere is the sheetness value at the center voxel, with all voxels
     * in the the resultant volume set to the average value of all spheres
     * containing that voxel.
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return Histogram maps class with sheetness histograms
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final <T extends IntegerType<T> & NativeType<T>> Map<List<Integer>, Long> projectCurvatureToSurface(
	    final JavaSparkContext sc, final String n5Path, final String datasetName, final String n5OutputPath,
	    final List<BlockInformation> blockInformationList) throws IOException {

	// General information
	final N5Reader n5Reader = new N5FSReader(n5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
	final int[] blockSize = attributes.getBlockSize();
	double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
	double voxelVolume = pixelResolution[0] * pixelResolution[1] * pixelResolution[2];
	double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

	// Create output that will contain volume averaged sheetness
	// TODO: check to make sure that for sphere to influence volume averaged
	// sheetness of voxel, it can't pass through background
	String outputDatasetName = datasetName + "_sheetnessVolumeAveraged";
	ProcessingHelper.createDatasetUsingTemplateDataset(n5Path, datasetName, n5OutputPath, outputDatasetName,
		DataType.UINT8);

	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<Map<List<Integer>, Long>> javaRDDHistogramMaps = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long padding = blockInformation.paddingForMedialSurface;
	    final long[] paddedOffset = blockInformation.getPaddedOffset(padding);
	    final long[] dimension = gridBlock[1];
	    final long[] paddedDimension = blockInformation.getPaddedDimension(padding);

	    // Create images required to calculate the average sheetness at a voxel: the sum
	    // and the counts at a given voxel. Add an extra 2 because need to check for
	    // surface voxels so need extra border of 1
	    final Img<FloatType> sheetnessSum = new ArrayImgFactory<FloatType>(new FloatType())
		    .create(new long[] { blockSize[0] + 2, blockSize[1] + 2, blockSize[2] + 2 });
	    final Img<UnsignedIntType> counts = new ArrayImgFactory<UnsignedIntType>(new UnsignedIntType())
		    .create(new long[] { blockSize[0] + 2, blockSize[1] + 2, blockSize[2] + 2 });
	    RandomAccess<FloatType> sheetnessSumRA = sheetnessSum.randomAccess();
	    RandomAccess<UnsignedIntType> countsRA = counts.randomAccess();

	    // Update sheetness and thickness histogram, as well as sum and counts used to
	    // create volume averaged sheetness
	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();

	    { // Necessary? scoping to limit memory
		// Get corresponding medial surface and sheetness
		Cursor<T> medialSurfaceCursor = ProcessingHelper.getOffsetIntervalExtendZeroC(n5Path,
			datasetName + "_medialSurface", paddedOffset, paddedDimension);
		RandomAccess<FloatType> sheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5Path,
			datasetName + "_sheetness", paddedOffset, paddedDimension);
		RandomAccess<FloatType> distanceTransformRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(
			n5OutputPath, datasetName + "_medialSurfaceDistanceTransform", paddedOffset, paddedDimension);

		createSumAndCountsAndUpdateHistogram(medialSurfaceCursor, distanceTransformRA, sheetnessRA,
			sheetnessSumRA, countsRA, pixelResolution, padding, paddedDimension,
			sheetnessAndThicknessHistogram);
	    }

	    // Take average
	    final Img<UnsignedByteType> output = createVolumeAveragedSheetnessAndUpdateHistograms(dimension,
		    sheetnessSumRA, countsRA, voxelVolume, voxelFaceArea);

	    // Write out volume averaged sheetness
	    final N5FSWriter n5BlockWriter = new N5FSWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);
	    return sheetnessAndThicknessHistogram;
	});

	Map<List<Integer>, Long> sheetnessAndThicknessHistogram = javaRDDHistogramMaps.reduce((a, b) -> {
	    for (Entry<List<Integer>, Long> entry : b.entrySet()) {
		a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0L) + entry.getValue());
	    }
	    return a;
	});

	return sheetnessAndThicknessHistogram;

    }

    /**
     * 
     * Calculate sheetness and volume histograms
     * 
     * @param sc                   Spark context
     * @param n5Path               Path to n5
     * @param datasetName          N5 dataset name
     * @param n5OutputPath         Path to output n5
     * @param blockInformationList Block inforation list
     * @return Histogram maps class with sheetness histograms
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static final HistogramMaps calculateSheetnessAndVolume(final JavaSparkContext sc, final String datasetName,
	    final String n5OutputPath, final List<BlockInformation> blockInformationList) throws IOException {

	// General information
	final N5Reader n5Reader = new N5FSReader(n5OutputPath);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(datasetName);
	final long[] dimensions = attributes.getDimensions();
	double[] pixelResolution = IOHelper.getResolution(n5Reader, datasetName);
	double voxelVolume = pixelResolution[0] * pixelResolution[1] * pixelResolution[2];
	double voxelFaceArea = pixelResolution[0] * pixelResolution[1];

	// Create output that will contain volume averaged sheetness

	// Parallelize analysis over blocks
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	JavaRDD<HistogramMaps> javaRDDHistogramMaps = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    final long[] dimension = gridBlock[1];
	    final long[] paddedOffset = blockInformation.getPaddedOffset(1);
	    final long[] paddedDimension = blockInformation.getPaddedDimension(1);

	    final N5Reader n5BlockReader = new N5FSReader(n5OutputPath);

	    // Get corresponding medial surface and sheetness
	    RandomAccess<UnsignedByteType> sheetnessRA = ProcessingHelper.getOffsetIntervalExtendZeroRA(n5OutputPath,
		    datasetName, paddedOffset, paddedDimension);

	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram = new HashMap<List<Integer>, Long>();
	    Map<Integer, Double> sheetnessAndSurfaceAreaHistogram = new HashMap<Integer, Double>();
	    Map<Integer, Double> sheetnessAndVolumeHistogram = new HashMap<Integer, Double>();

	    for (long x = 1; x < dimension[0] + 1; x++) {
		for (long y = 1; y < dimension[1] + 1; y++) {
		    for (long z = 1; z < dimension[2] + 1; z++) {
			long[] pos = new long[] { x, y, z };
			sheetnessRA.setPosition(pos);
			int sheetnessMeasureBin = sheetnessRA.get().get();
			if (sheetnessMeasureBin > 0) {
			    sheetnessAndVolumeHistogram.put(sheetnessMeasureBin,
				    sheetnessAndVolumeHistogram.getOrDefault(sheetnessMeasureBin, 0.0) + voxelVolume);
			    int faces = ProcessingHelper.getSurfaceAreaContributionOfVoxelInFaces(sheetnessRA,
				    paddedOffset, dimensions, false);
			    if (faces > 0) {
				sheetnessAndSurfaceAreaHistogram.put(sheetnessMeasureBin,
					sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessMeasureBin, 0.0)
					+ faces * voxelFaceArea);
			    }
			}
		    }
		}
	    }
	    return new HistogramMaps(sheetnessAndThicknessHistogram, sheetnessAndSurfaceAreaHistogram,
		    sheetnessAndVolumeHistogram);
	});

	HistogramMaps histogramMaps = javaRDDHistogramMaps.reduce((a, b) -> {
	    a.merge(b);
	    return a;
	});

	return histogramMaps;

    }

    /**
     * Update sheetness sum and count images which are used to calculate the volume
     * averaged sheetness. The averaged sheetness is calculated by taking the radius
     * (thickness) at a medial surface voxel, and filing in all voxels within that
     * sphere with the corresponding sheetness. Sum and counts are used to take the
     * average in cases where multiple spheres contain a single voxel.
     * 
     * @param pos               Position
     * @param radiusPlusPadding Radius with padding
     * @param radiusSquared     Radius squared
     * @param cpdt              Correctly padded distance transform instance
     * @param sheetnessMeasure  Sheetness value
     * @param sheetnessSumRA    Random access for sheetness sum
     * @param countsRA          Random access for counts
     */
    private static void updateSheetnessSumAndCount(int[] pos, int radiusPlusPadding, float radiusSquared,
	    float sheetnessMeasure, RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA,
	    long padding, long[] paddedDimension) {

	for (int x = pos[0] - radiusPlusPadding; x <= pos[0] + radiusPlusPadding; x++) {
	    for (int y = pos[1] - radiusPlusPadding; y <= pos[1] + radiusPlusPadding; y++) {
		for (int z = pos[2] - radiusPlusPadding; z <= pos[2] + radiusPlusPadding; z++) {
		    int dx = x - pos[0];
		    int dy = y - pos[1];
		    int dz = z - pos[2];
		    // need to check extra padding of 1 because in next step we need this halo for
		    // checking surfaces
		    if ((x >= padding - 1 && x <= paddedDimension[0] - padding && y >= padding - 1
			    && y <= paddedDimension[1] - padding && z >= padding - 1
			    && z <= paddedDimension[2] - padding) && dx * dx + dy * dy + dz * dz <= radiusSquared) {
			// then it is in sphere
			long[] spherePos = new long[] { x - (padding - 1), y - (padding - 1), z - (padding - 1) };

			sheetnessSumRA.setPosition(spherePos);
			FloatType outputVoxel = sheetnessSumRA.get();
			outputVoxel.set(outputVoxel.get() + sheetnessMeasure);

			countsRA.setPosition(spherePos);
			UnsignedIntType countsVoxel = countsRA.get();
			countsVoxel.set(countsVoxel.get() + 1);

		    }
		}
	    }
	}
    }

    /**
     * Use distance transform to get thickness at medial surface. Use that thickness
     * as a radius for spheres to calculate sum and counts for volume averaged
     * sheetness.
     * 
     * @param medialSurfaceCursor            Cursor for medial surface
     * @param cpdt                           Correctly padded distance transform
     *                                       class instance
     * @param sheetnessRA                    Sheetness random access
     * @param sheetnessSumRA                 Sheetness sum random access
     * @param countsRA                       Counts random access
     * @param pixelResolution                Pixel resolution
     * @param sheetnessAndThicknessHistogram Histogram as a map containing sheetness
     *                                       and thickness
     */
    private static <T extends IntegerType<T> & NativeType<T>> void createSumAndCountsAndUpdateHistogram(
	    Cursor<T> medialSurfaceCursor, RandomAccess<FloatType> distanceTransformRA,
	    RandomAccess<FloatType> sheetnessRA, RandomAccess<FloatType> sheetnessSumRA,
	    RandomAccess<UnsignedIntType> countsRA, double[] pixelResolution, long padding, long[] paddedDimension,
	    Map<List<Integer>, Long> sheetnessAndThicknessHistogram) {
	while (medialSurfaceCursor.hasNext()) {
	    final long medialSurfaceValue = medialSurfaceCursor.next().getIntegerLong();
	    if (medialSurfaceValue > 0) { // then it is on medial surface

		int[] pos = { medialSurfaceCursor.getIntPosition(0), medialSurfaceCursor.getIntPosition(1),
			medialSurfaceCursor.getIntPosition(2) };
		distanceTransformRA.setPosition(pos);
		sheetnessRA.setPosition(pos);

		float radiusSquared = distanceTransformRA.get().getRealFloat();
		double radius = Math.sqrt(radiusSquared);
		int radiusPlusPadding = (int) Math.ceil(radius);

		float sheetnessMeasure = sheetnessRA.get().getRealFloat();
		int sheetnessMeasureBin = (int) Math.ceil(sheetnessMeasure * 254) + 1;

		if (pos[0] >= padding && pos[0] < paddedDimension[0] - padding && pos[1] >= padding
			&& pos[1] < paddedDimension[1] - padding && pos[2] >= padding
			&& pos[2] < paddedDimension[2] - padding) {

		    double thickness = radius * 2;// convert later
		    // bin thickness in 8 nm bins
		    int thicknessBin = (int) Math.min(Math.floor(thickness * pixelResolution[0] / 8), 99);

		    List<Integer> histogramBinList = Arrays.asList(sheetnessMeasureBin, thicknessBin);
		    sheetnessAndThicknessHistogram.put(histogramBinList,
			    sheetnessAndThicknessHistogram.getOrDefault(histogramBinList, 0L) + 1L);
		}

		updateSheetnessSumAndCount(pos, radiusPlusPadding, radiusSquared, sheetnessMeasure, sheetnessSumRA,
			countsRA, padding, paddedDimension);
	    }
	}
    }

    /**
     * Create output volume averaged sheetness by dividing Sum image by Counts
     * image, and binning from 0-255.
     * 
     * @param dimension                        Block dimension
     * @param sheetnessSumRA                   Sheetness sum random access
     * @param countsRA                         Counts random access
     * @param voxelVolume                      Volume of a voxel
     * @param voxelFaceArea                    Area of a voxel face
     * @param sheetnessAndSurfaceAreaHistogram Histogram for sheetness and surface
     *                                         area
     * @param sheetnessAndVolumeHistogram      Histogram for sheetness and volume
     * @return
     */
    private static Img<UnsignedByteType> createVolumeAveragedSheetnessAndUpdateHistograms(long[] dimension,
	    RandomAccess<FloatType> sheetnessSumRA, RandomAccess<UnsignedIntType> countsRA, double voxelVolume,
	    double voxelFaceArea) {

	final Img<UnsignedByteType> output = new ArrayImgFactory<UnsignedByteType>(new UnsignedByteType())
		.create(dimension);
	RandomAccess<UnsignedByteType> outputRandomAccess = output.randomAccess();
	for (long x = 1; x < dimension[0] + 1; x++) {
	    for (long y = 1; y < dimension[1] + 1; y++) {
		for (long z = 1; z < dimension[2] + 1; z++) {

		    long[] pos = new long[] { x, y, z };
		    countsRA.setPosition(pos);
		    if (countsRA.get().get() > 0) {
			sheetnessSumRA.setPosition(pos);
			float sheetnessMeasure = sheetnessSumRA.get().get() / countsRA.get().get();
			sheetnessSumRA.get().set(sheetnessMeasure);// take average
			int sheetnessMeasureBin = (int) Math.ceil(sheetnessMeasure * 254) + 1;// add one in case any are
			// 0

			outputRandomAccess.setPosition(new long[] { x - 1, y - 1, z - 1 });

			// rescale to 0-255
			outputRandomAccess.get().set(sheetnessMeasureBin);
		    }
		}
	    }
	}
	return output;
    }

    /**
     * Function to write out histograms.
     * 
     * @param histogramMaps   Maps containing histograms
     * @param outputDirectory Output directory to write files to
     * @param datasetName     Dataset name that is being analyzed
     * @throws IOException
     */
    public static void writeData(HistogramMaps histogramMaps, String outputDirectory, String datasetName,
	    boolean writeThickness) throws IOException {
	if (!new File(outputDirectory).exists()) {
	    new File(outputDirectory).mkdirs();
	}

	FileWriter sheetnessVolumeAndAreaHistograms = new FileWriter(
		outputDirectory + "/" + datasetName + "_sheetnessVolumeAndAreaHistograms.csv");
	sheetnessVolumeAndAreaHistograms.append("Sheetness,Volume (nm^3),Surface Area(nm^2)\n");

	FileWriter sheetnessVsThicknessHistogram = null;
	String rowString;
	if (writeThickness) {
	    sheetnessVsThicknessHistogram = new FileWriter(
		    outputDirectory + "/" + datasetName + "_sheetnessVsThicknessHistogram.csv");
	    rowString = "Sheetness/Thickness (nm)";
	    for (int thicknessBin = 0; thicknessBin < 100; thicknessBin++) {
		rowString += "," + Integer.toString(thicknessBin * 8 + 4);
	    }
	    sheetnessVsThicknessHistogram.append(rowString + "\n");
	}

	for (int sheetnessBin = 1; sheetnessBin < 256; sheetnessBin++) {
	    double volume = histogramMaps.sheetnessAndVolumeHistogram.getOrDefault(sheetnessBin, 0.0);
	    double surfaceArea = histogramMaps.sheetnessAndSurfaceAreaHistogram.getOrDefault(sheetnessBin, 0.0);

	    String sheetnessBinString = Double.toString((sheetnessBin - 1) / 255.0 + 0.5 / 255.0);
	    sheetnessVolumeAndAreaHistograms.append(
		    sheetnessBinString + "," + Double.toString(volume) + "," + Double.toString(surfaceArea) + "\n");

	    if (writeThickness) {
		rowString = sheetnessBinString;
		for (int thicknessBin = 0; thicknessBin < 100; thicknessBin++) {
		    double thicknessCount = histogramMaps.sheetnessAndThicknessHistogram
			    .getOrDefault(Arrays.asList(sheetnessBin, thicknessBin), 0L);
		    rowString += "," + Double.toString(thicknessCount);
		}
		sheetnessVsThicknessHistogram.append(rowString + "\n");
	    }
	}
	sheetnessVolumeAndAreaHistograms.flush();
	sheetnessVolumeAndAreaHistograms.close();

	if (writeThickness) {
	    sheetnessVsThicknessHistogram.flush();
	    sheetnessVsThicknessHistogram.close();
	}

    }

    public static void setupSparkAndCalculatePropertiesFromMedialSurface(String inputN5Path, String inputN5DatasetName,
	    String outputN5Path, String outputDirectory, boolean calculateAreaAndVolumeFromExistingDataset)
		    throws IOException {

	final SparkConf conf = new SparkConf().setAppName("SparkCalculatePropertiesOfMedialSurface");

	for (String organelle : inputN5DatasetName.split(",")) {
	    List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path,
		    organelle);

	    JavaSparkContext sc = new JavaSparkContext(conf);
	    if (calculateAreaAndVolumeFromExistingDataset) {
		HistogramMaps histogramMaps = calculateSheetnessAndVolume(sc, organelle, outputN5Path,
			blockInformationList);
		writeData(histogramMaps, outputDirectory, organelle, false);
	    } else {
		HistogramMaps histogramMaps = calculatePropertiesFromMedialSurface(sc, inputN5Path, organelle,
			outputN5Path, blockInformationList);
		writeData(histogramMaps, outputDirectory, organelle, true);
	    }

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
	String outputN5Path = options.getOutputN5Path();
	String outputDirectory = options.getOutputDirectory();
	boolean calculateAreaAndVolumeFromExistingDataset = options.getCalculateAreaAndVolumeFromExistingDataset();

	setupSparkAndCalculatePropertiesFromMedialSurface(inputN5Path, inputN5DatasetName, outputN5Path,
		outputDirectory, calculateAreaAndVolumeFromExistingDataset);

    }

}
