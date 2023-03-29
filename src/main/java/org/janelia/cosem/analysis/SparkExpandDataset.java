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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.cosem.util.ProcessingHelper;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import static org.janelia.cosem.util.N5GenericReaderWriter.*;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import ij.ImagePlus;
import net.imagej.ImageJ;
import net.imagej.ops.convert.ConvertImages.Uint8;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.Dilation;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;

import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

import net.imglib2.algorithm.morphology.StructuringElements;

/**
 * Expand dataset
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkExpandDataset {
    @SuppressWarnings("serial")
    public static class Options extends AbstractOptions implements Serializable {

	@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
	private String inputN5Path = null;

	@Option(name = "--outputN5Path", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
	private String outputN5Path = null;

	@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
	private String inputN5DatasetName = null;

	@Option(name = "--outputN5DatasetSuffix", required = false, usage = "N5 suffix, e.g. _expandedForMeshes")
	private String outputN5DatasetSuffix = "_expanded";

	@Option(name = "--thresholdIntensityCutoff", required = false, usage = "Threshold intensity cutoff above which objects will be expanded")
	private Integer thresholdIntensityCutoff = 0;

	@Option(name = "--expansionInNm", required = false, usage = "Expansion (nm)")
	private double expansionInNm = 12;

	@Option(name = "--useFixedValue", required = false, usage = "Whether to use a fixed value of 255 for expanded objects")
	private boolean useFixedValue = false;

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

	public String getOutputN5Path() {
	    if (outputN5Path == null) {
		return inputN5Path;
	    } else {
		return outputN5Path;
	    }
	}

	public String getInputN5DatasetName() {
	    return inputN5DatasetName;
	}

	public String getOutputN5DatasetSuffix() {
	    return outputN5DatasetSuffix;
	}

	public Integer getThresholdIntensityCutoff() {
	    return thresholdIntensityCutoff;
	}

	public double getExpansionInNm() {
	    return expansionInNm;
	}

	public boolean getUseFixedValue() {
	    return useFixedValue;
	}

    }

    public static ArrayList<long[]> getRelativeOffsetsToCheck(long[] dimension, double expansionInVoxelsSquared) {
	long dx = dimension[0];
	long dy = dimension[1];
	long dz = dimension[2];

	// subtract 1 because goes from eg. 0-127 for dimensions 128
	long[] xs = new long[] { 0, dx - 1 };
	long[] ys = new long[] { 0, dy - 1 };
	long[] zs = new long[] { 0, dz - 1 };
	ArrayList<long[]> corners = new ArrayList<long[]>();
	for (long xCorner : xs) {
	    for (long yCorner : ys) {
		for (long zCorner : zs) {
		    corners.add(new long[] { xCorner, yCorner, zCorner });
		}
	    }
	}

	int padding = 1;
	boolean added = true;
	ArrayList<long[]> relativeOffsets = new ArrayList<long[]>();
	// the original block; -1 indicates completed
	relativeOffsets.add(new long[] { 0, 0, 0, -1 });
	while (added) {
	    added = false;
	    for (long xNew = -dx * padding; xNew <= dx * padding; xNew += dx) {
		for (long yNew = -dy * padding; yNew <= dy * padding; yNew += dy) {
		    for (long zNew = -dz * padding; zNew <= dz * padding; zNew += dz) {
			if (xNew == -dx * padding || xNew == dx * padding || yNew == -dy * padding
				|| yNew == dy * padding || zNew == -dz * padding || zNew == dz * padding) {
			    // then these are the next level out
			    ArrayList<long[]> newCorners = new ArrayList<long[]>();
			    for (long xNewCorner : new long[] { xNew, xNew + dx - 1 }) {
				for (long yNewCorner : new long[] { yNew, yNew + dy - 1 }) {
				    for (long zNewCorner : new long[] { zNew, zNew + dz - 1 }) {
					newCorners.add(new long[] { xNewCorner, yNewCorner, zNewCorner });
				    }
				}
			    }

			    double maxCornerDistanceSquared = 0;
			    boolean addThisBlock = false;
			    for (long[] corner : corners) {
				for (long[] newCorner : newCorners) {
				    double cornerDistanceSquared = Math.pow(newCorner[0] - corner[0], 2)
					    + Math.pow(newCorner[1] - corner[1], 2)
					    + Math.pow(newCorner[2] - corner[2], 2);
				    if (cornerDistanceSquared > maxCornerDistanceSquared)
					maxCornerDistanceSquared = cornerDistanceSquared;
				    if (cornerDistanceSquared < expansionInVoxelsSquared) {
					// # if it is close
					added = true;
					addThisBlock = true;
					// count+=1
				    }
				}
			    }

			    if (addThisBlock) {
				if (maxCornerDistanceSquared < expansionInVoxelsSquared) {
				    // 0 indicates it can be completely filled
				    relativeOffsets.add(new long[] { xNew, yNew, zNew, 0 });
				} else {
				    // 1 indicates we need to do the actual distance transform
				    relativeOffsets.add(new long[] { xNew, yNew, zNew, 1 });
				}
			    }
			}
		    }
		}
	    }
	    padding++;
	}
	return relativeOffsets;
    }

    public static Set<List<Long>> getOffsetsToCheckNext(ArrayList<long[]> relativeOffsets, long[] offset) {
	Set<List<Long>> offsetsToCheckNext = new HashSet<List<Long>>();
	for (int r=0; r<relativeOffsets.size(); r++) {
	    long [] currentRelativeOffset = relativeOffsets.get(r).clone();
	    for (int i = 0; i < 3; i++) {
		currentRelativeOffset[i] += offset[i];
	    }
	    offsetsToCheckNext.add(Arrays.stream(currentRelativeOffset).boxed().collect(Collectors.toList()));
	}
	return offsetsToCheckNext;
    }

    public static class BlockInformationListsToRerun {
	public final List<BlockInformation> fillBlockInformationList;
	public final List<BlockInformation> distanceTransformBlockInformationList;

	public BlockInformationListsToRerun(List<BlockInformation> fillBlockInformationList,
		List<BlockInformation> distanceTransformBlockInformationList) {
	    this.fillBlockInformationList = fillBlockInformationList;
	    this.distanceTransformBlockInformationList = distanceTransformBlockInformationList;
	}
    }

    /**
     * Fill any chunk containing a value
     * 
     * @param sc                   Spark context
     * @param n5Path               Input N5 path
     * @param inputDatasetName     Skeletonization dataset name
     * @param n5OutputPath         Output N5 path
     * @param outputDatasetName    Output N5 dataset name
     * @param expansionInNm        Expansion in Nm
     * @param blockInformationList List of block information
     * @throws IOException
     */
    public static final <T extends IntegerType<T> & NativeType<T>> BlockInformationListsToRerun fillChunksContainingObjects(
	    final JavaSparkContext sc, final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    final String outputDatasetName, final int thresholdIntensity, final double expansionInNm,
	    List<BlockInformation> blockInformationList) throws IOException {

	final N5Reader n5Reader = N5GenericReader(n5Path);

	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
	final long[] dimensions = attributes.getDimensions();
	final int[] blockSize = attributes.getBlockSize();

	long[] templateDimension = new long[] { blockSize[0], blockSize[1], blockSize[2] };
//	IntervalView<UnsignedByteType> filledBlock = ProcessingHelper.getZerosIntegerImageRAI(templateDimension,
//		DataType.UINT8);
//	Cursor<UnsignedByteType> filledBlockCursor = filledBlock.cursor();
//	while (filledBlockCursor.hasNext()) {
//	    filledBlockCursor.next().set(255);
//	}
	final N5Writer n5Writer = N5GenericWriter(n5OutputPath);

	n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader, inputDatasetName));

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

	// create blank white and full black of default size
	double expansionInVoxels = expansionInNm / pixelResolution[0];
	double expansionInVoxelsSquared = expansionInVoxels * expansionInVoxels;
	final ArrayList<long[]> relativeOffsetsToCheck = getRelativeOffsetsToCheck(templateDimension,
		expansionInVoxelsSquared);
	JavaRDD<Set<List<Long>>> blockInformationRDD = rdd.map(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];// new long[] {64,64,64};//gridBlock[0];////
	    long[] dimension = gridBlock[1];

	    final N5Reader n5BlockReader = N5GenericReader(n5Path);

	    IntervalView<T> dataset = ProcessingHelper.getOffsetIntervalExtendZeroRAI(n5Path, inputDatasetName, offset,
		    dimension);
	    Cursor<T> datasetCursor = dataset.cursor();

	    // if expansionInVoxelsCeiling
	    boolean hasObject = false;
	    while (datasetCursor.hasNext() && !hasObject) {
		if (datasetCursor.next().getIntegerLong() > 0)
		    hasObject = true;
	    }

	    Set<List<Long>> blockOffsetsToCheckNext = new HashSet<List<Long>>();
	    final N5Writer n5BlockWriter = N5GenericWriter(n5OutputPath);
	    if (hasObject) {
		IntervalView<UnsignedByteType> customFilledBlock = ProcessingHelper
			.getZerosIntegerImageRAI(templateDimension, DataType.UINT8);
		Cursor<UnsignedByteType> customFilledBlockCursor = customFilledBlock.cursor();
		while (customFilledBlockCursor.hasNext()) {
		    customFilledBlockCursor.next().set(255);
		}
		N5Utils.saveBlock(customFilledBlock, n5BlockWriter, outputDatasetName, gridBlock[2]);

		blockOffsetsToCheckNext = getOffsetsToCheckNext(relativeOffsetsToCheck, offset);
	    } else {
		if (attributes.getDataType() == DataType.UINT8) {
		    N5Utils.saveBlock(dataset, n5BlockWriter, outputDatasetName, gridBlock[2]);
		} else {
		    IntervalView<UnsignedByteType> customEmptyBlock = ProcessingHelper
			    .getZerosIntegerImageRAI(templateDimension, DataType.UINT8);
		    Cursor<UnsignedByteType> customEmptyBlockCursor = customEmptyBlock.cursor();
		    while (customEmptyBlockCursor.hasNext()) {
			customEmptyBlockCursor.next().set(0);
		    }
		    N5Utils.saveBlock(customEmptyBlock, n5BlockWriter, outputDatasetName, gridBlock[2]);
		}
	    }
	    return blockOffsetsToCheckNext;
	    // create set of offsets that are valid, then we can go through blocks and pop
	    // off blocks that dont fit
	});
	Set<List<Long>> blockOffsetsToCheckNext = blockInformationRDD.reduce((a, b) -> {
	    // priority: -1 (done) > 0 (can be filled) > 1 (needs distance transform)
	    a.addAll(b);

	    /*
	     * if we want to minimize the size of the list by a factor of at most 3, we can
	     * do more complex set merging for(List<Long> currentBlockOffset: b) {
	     * if(!a.contains(currentBlockOffset)) { long fillableStatus =
	     * currentBlockOffset.get(3); if(fillableStatus == -1) { //highest priority
	     * a.remove(Arrays.asList(currentBlockOffset.get(0),currentBlockOffset.get(1),
	     * currentBlockOffset.get(2),1));
	     * a.remove(Arrays.asList(currentBlockOffset.get(0),currentBlockOffset.get(1),
	     * currentBlockOffset.get(2),0)); a.add(currentBlockOffset); } else if
	     * (fillableStatus == 0 &&
	     * !a.contains(Arrays.asList(currentBlockOffset.get(0),currentBlockOffset.get(1)
	     * ,currentBlockOffset.get(2),-1))){
	     * a.remove(Arrays.asList(currentBlockOffset.get(0),currentBlockOffset.get(1),
	     * currentBlockOffset.get(2),1)); a.add(currentBlockOffset); } }
	     * 
	     * }
	     * 
	     * }
	     */
	    return a;
	});

	List<BlockInformation> fillBlockInformationList = new ArrayList<BlockInformation>();
	List<BlockInformation> distanceTransformBlockInformationList = new ArrayList<BlockInformation>();
	for (BlockInformation blockInformation : blockInformationList) {
	    List<Long> filled = Arrays.asList(blockInformation.gridBlock[0][0], blockInformation.gridBlock[0][1],
		    blockInformation.gridBlock[0][2], -1L);
	    List<Long> needsToBeFilled = Arrays.asList(blockInformation.gridBlock[0][0],
		    blockInformation.gridBlock[0][1], blockInformation.gridBlock[0][2], 0L);
	    List<Long> needsToBeDistanceTransformed = Arrays.asList(blockInformation.gridBlock[0][0],
		    blockInformation.gridBlock[0][1], blockInformation.gridBlock[0][2], 1L);
	    if (!blockOffsetsToCheckNext.contains(filled)) {
		if (blockOffsetsToCheckNext.contains(needsToBeFilled)) {
		    fillBlockInformationList.add(blockInformation);
		} else if (blockOffsetsToCheckNext.contains(needsToBeDistanceTransformed)) {
		    distanceTransformBlockInformationList.add(blockInformation);
		}
	    }
	}

	return new BlockInformationListsToRerun(fillBlockInformationList, distanceTransformBlockInformationList);
    }

    public static final <T extends IntegerType<T> & NativeType<T>> void fillChunks(final JavaSparkContext sc,
	    final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    final String outputDatasetName, List<BlockInformation> blockInformationList) throws IOException {

	final N5Reader n5Reader = N5GenericReader(n5Path);

	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
	final long[] dimensions = attributes.getDimensions();
	final int[] blockSize = attributes.getBlockSize();
	final N5Writer n5Writer = N5GenericWriter(n5OutputPath);

	n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader, inputDatasetName));

	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] dimension = gridBlock[1];
	    final N5Writer n5BlockWriter = N5GenericWriter(n5OutputPath);
	    IntervalView<UnsignedByteType> customFilledBlock = ProcessingHelper.getZerosIntegerImageRAI(dimension,
		    DataType.UINT8);
	    Cursor<UnsignedByteType> customFilledBlockCursor = customFilledBlock.cursor();
	    while (customFilledBlockCursor.hasNext()) {
		customFilledBlockCursor.next().set(255);
	    }
	    N5Utils.saveBlock(customFilledBlock, n5BlockWriter, outputDatasetName, gridBlock[2]);

	});
    }

    public static final double squaredDistanceFromBox(long[] pos, long padding, long[] dimension) {
	// https://math.stackexchange.com/questions/2133217/minimal-distance-to-a-cube-in-2d-and-3d-from-a-point-lying-outside
	double distSquared = 0,delta=0;
	long edge1=padding, edge2;
	for (int i = 0; i < 3; i++) {
	    edge2=dimension[i]-padding-1;
	    if(!(pos[i]>=edge1 && pos[i]<=edge2)) {
		delta=Math.min(Math.abs(pos[i]-edge1),Math.abs(pos[i]-edge2));
		distSquared+=Math.pow(delta,2);
	    }
	}
	return distSquared;
    }

    public static final long[] getCorrectPositionOrder(long dim1, long dim2, long dim3, int dim) {
	if (dim == 0) {
	    return new long[] { dim3, dim1, dim2 };
	} else if (dim == 1) {
	    return new long[] { dim1, dim3, dim2 };
	} else {
	    return new long[] { dim1, dim2, dim3 };
	}
    }

    public static final <T extends IntegerType<T> & NativeType<T>> long[] getMinAndMaxAlongDimension(
	    RandomAccessibleInterval<T> dataset, int dim, long[] dimension, long padding,
	    double expansionInVoxelsSquared) {
	RandomAccess<T> datasetRA = dataset.randomAccess();

	boolean foundMin = false, foundMax = false;
	long moveInAmount = 0, minCorrection = padding, maxCorrection = dimension[dim] - padding - 1;
	long[] pos;
	long dim3;

	int[] nonFixedDimensions = new int[] { -1, -1 };
	int counter = 0;
	for (int i = 0; i < 3; i++) {
	    if (i != dim) {
		nonFixedDimensions[counter] = i;
		counter++;
	    }
	}
	while (!(foundMin && foundMax) && moveInAmount < padding) {
	    outerLoop: for (long dim1 = 0; dim1 < dimension[nonFixedDimensions[0]]; dim1++) {
		for (long dim2 = 0; dim2 < dimension[nonFixedDimensions[1]]; dim2++) {
		    if (!foundMin) {
			dim3 = moveInAmount;
			pos = getCorrectPositionOrder(dim1, dim2, dim3, dim);
			datasetRA.setPosition(pos);
			if (datasetRA.get().getIntegerLong() > 0
				&& squaredDistanceFromBox(pos, padding, dimension) <= expansionInVoxelsSquared) {
			    foundMin = true;
			    minCorrection = dim3;
			}
		    }
		    if (!foundMax) {
			dim3 = dimension[dim] - 1 - moveInAmount;
			pos = getCorrectPositionOrder(dim1, dim2, dim3, dim);
			datasetRA.setPosition(pos);
			if (datasetRA.get().getIntegerLong() > 0
				&& squaredDistanceFromBox(pos, padding, dimension) <= expansionInVoxelsSquared) {
			    foundMax = true;
			    maxCorrection = dim3;
			}
		    }
		    if (foundMin && foundMax)
			break outerLoop;
		}
	    }
	    moveInAmount++;
	}
	return new long[] { minCorrection, maxCorrection };

    }

    public static final <T extends IntegerType<T> & NativeType<T>> long[][] getAdjustedOffsetAndDimension(
	    RandomAccessibleInterval<T> dataset, long[] dimension, long padding, double expansionInVoxelsSquared) {
	long[][] adjustedOffsetAndDimension = new long[2][3];
	for (int dim = 0; dim < 3; dim++) {
	    long[] minAndMaxAlongDimension = getMinAndMaxAlongDimension(dataset, dim, dimension, padding,
		    expansionInVoxelsSquared);
	    System.out.println(Arrays.toString(minAndMaxAlongDimension));
	    adjustedOffsetAndDimension[0][dim] = minAndMaxAlongDimension[0];
	    adjustedOffsetAndDimension[1][dim] = minAndMaxAlongDimension[1] - minAndMaxAlongDimension[0] + 1;
	}
	return adjustedOffsetAndDimension;
    }

    /**
     * Expand dataset
     * 
     * @param sc                   Spark context
     * @param n5Path               Input N5 path
     * @param inputDatasetName     Skeletonization dataset name
     * @param n5OutputPath         Output N5 path
     * @param outputDatasetName    Output N5 dataset name
     * @param expansionInNm        Expansion in Nm
     * @param blockInformationList List of block information
     * @throws IOException
     */
    public static final <T extends IntegerType<T> & NativeType<T>> void expandDataset(final JavaSparkContext sc,
	    final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    final String outputDatasetName, final int thresholdIntensity, final double expansionInNm,
	    final List<BlockInformation> blockInformationList, final boolean useFixedValue) throws IOException {

	final N5Reader n5Reader = N5GenericReader(n5Path);

	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
	final long[] dimensions = attributes.getDimensions();
	final int[] blockSize = attributes.getBlockSize();

	final N5Writer n5Writer = N5GenericWriter(n5OutputPath);

	if (useFixedValue) {
	    n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());
	} else {
	    n5Writer.createDataset(outputDatasetName, dimensions, blockSize, attributes.getDataType(),
		    new GzipCompression());
	}
		
	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader, inputDatasetName));

	// n5Writer.setAttribute(outputDatasetName, "offset",
	// n5Reader.getAttribute(inputDatasetName, "offset", int[].class));

	double expansionInVoxels = expansionInNm / pixelResolution[0];
	int expansionInVoxelsCeil = (int) Math.ceil(expansionInVoxels);
	double expansionInVoxelsSquared = expansionInVoxels * expansionInVoxels;
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];// new long[] {64,64,64};//gridBlock[0];////
	    long[] dimension = gridBlock[1];
	    long[] paddedOffset = new long[] { offset[0] - expansionInVoxelsCeil, offset[1] - expansionInVoxelsCeil,
		    offset[2] - expansionInVoxelsCeil };
	    long[] paddedDimension = new long[] { dimension[0] + 2 * expansionInVoxelsCeil,
		    dimension[1] + 2 * expansionInVoxelsCeil, dimension[2] + 2 * expansionInVoxelsCeil };
	    final N5Reader n5BlockReader = N5GenericReader(n5Path);

	    //ProcessingHelper.logMemory("about to read ");
	    RandomAccessibleInterval<T> expandedDataset = Views.offsetInterval(
		    Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, inputDatasetName)),
		    paddedOffset, paddedDimension);

	    long[][] adjustedOffsetAndDimension = getAdjustedOffsetAndDimension(expandedDataset, paddedDimension,
		    expansionInVoxelsCeil, expansionInVoxelsSquared);
	    long[] adjustedOffset = adjustedOffsetAndDimension[0];
	    long[] boxOffset = new long[] { expansionInVoxelsCeil - adjustedOffset[0],
		    expansionInVoxelsCeil - adjustedOffset[1], expansionInVoxelsCeil - adjustedOffset[2] };
	    long[] adjustedDimension = adjustedOffsetAndDimension[1];
	    
	    //filler to remove
	    //boxOffset = new long[] {expansionInVoxelsCeil,expansionInVoxelsCeil,expansionInVoxelsCeil};
	    //adjustedOffset = new long[] {0,0,0};
	    //adjustedDimension=paddedDimension;
	    
	    expandedDataset = Views.offsetInterval(expandedDataset, adjustedOffset, adjustedDimension);
	    RandomAccess<T> expandedDatasetRA = expandedDataset.randomAccess();
	    //ProcessingHelper.logMemory("about to crop ");

	    //ProcessingHelper.logMemory("about to convert ");
	    RandomAccessibleInterval<NativeBoolType> converted = Converters.convert(expandedDataset, (a, b) -> {
		b.set(a.getIntegerLong() > thresholdIntensity);
	    }, new NativeBoolType());

	    // RandomAccessibleInterval<NativeBoolType> convertedTemp =
	    // ProcessingHelper.getFalsesBoolImageRAI(paddedDimension);

	    //ProcessingHelper.logMemory("about to create distance transform image ");
	    RandomAccessibleInterval<FloatType> distanceTransform = ArrayImgs.floats(adjustedDimension);
	    //ProcessingHelper.logMemory("about to create distance transform ");
	    DistanceTransform.binaryTransform(converted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
	    
	    //ProcessingHelper.logMemory("about to crop ");
	    distanceTransform = (RandomAccessibleInterval<FloatType>) Views.offsetInterval(distanceTransform, boxOffset,
		    dimension);
	    RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();
	    

	    RandomAccessibleInterval<T> dataset = (RandomAccessibleInterval<T>) Views.offsetInterval(expandedDataset,
		    boxOffset, dimension);
	    RandomAccess<T> datasetRA = dataset.randomAccess();
	    if (useFixedValue) {
		expandedDataset = null;
	    }
	    //ImageJFunctions.show(distanceTransform);
	    //ImageJFunctions.show(distanceTransform);
	    //ProcessingHelper.logMemory("about to loop ");
	    long newX, newY, newZ;
	    for (long x = boxOffset[0]; x < boxOffset[0] + dimension[0]; x++) {
		for (long y = boxOffset[1]; y < boxOffset[1] + dimension[1]; y++) {
		    for (long z = boxOffset[2]; z < boxOffset[2] + dimension[2]; z++) {
			long pos[] = new long[] { x - boxOffset[0], y - boxOffset[1], z - boxOffset[2] };
			distanceTransformRA.setPosition(pos);
			float distanceSquared = distanceTransformRA.get().get();
			if (distanceSquared <= expansionInVoxelsSquared) {
			    datasetRA.setPosition(pos);
			    if (useFixedValue) {
				datasetRA.get().setInteger(255);
			    } else {
				Set<List<Integer>> voxelsToCheck = SparkContactSites
					.getVoxelsToCheckBasedOnDistance(distanceSquared);
				for (List<Integer> voxelToCheck : voxelsToCheck) {
				    newX = x + voxelToCheck.get(0);
				    newY = y + voxelToCheck.get(1);
				    newZ = z + voxelToCheck.get(2);
				    if (newX >= 0 && newX < adjustedDimension[0] && newY >= 0
					    && newY < adjustedDimension[1] && newZ >= 0
					    && newZ < adjustedDimension[2]) {
					expandedDatasetRA.setPosition(new long[] { newX, newY, newZ });
					T currentObjectID = expandedDatasetRA.get();
					if (currentObjectID.getIntegerLong() > 0) {
					    datasetRA.get().set(currentObjectID);
					    break;
					}
				    }
				}
			    }
			}
		    }
		}
	    }
	    //ProcessingHelper.logMemory("looper ");
	    final N5Writer n5BlockWriter = N5GenericWriter(n5OutputPath);
	    if (useFixedValue) {
		final RandomAccessibleInterval<UnsignedByteType> datasetConverted = Converters.convert(dataset,
			(a, b) -> {
			    b.set(a.getIntegerLong() > 0 ? 255 : 0);
			}, new UnsignedByteType());
		N5Utils.saveBlock(datasetConverted, n5BlockWriter, outputDatasetName, gridBlock[2]);
	    } else {
		N5Utils.saveBlock(dataset, n5BlockWriter, outputDatasetName, gridBlock[2]);
	    }

	});

    }

    /**
     * Expand dataset using fixed value
     * 
     * @param sc                   Spark context
     * @param n5Path               Input N5 path
     * @param inputDatasetName     Skeletonization dataset name
     * @param n5OutputPath         Output N5 path
     * @param outputDatasetName    Output N5 dataset name
     * @param expansionInNm        Expansion in Nm
     * @param blockInformationList List of block information
     * @throws IOException
     */
    public static final <T extends IntegerType<T> & NativeType<T>> void expandDatasetUsingFixedValue(
	    final JavaSparkContext sc, final String n5Path, final String inputDatasetName, final String n5OutputPath,
	    final String outputDatasetName, final int thresholdIntensity, final double expansionInNm,
	    final List<BlockInformation> blockInformationList) throws IOException {

	final N5Reader n5Reader = N5GenericReader(n5Path);

	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
	final long[] dimensions = attributes.getDimensions();
	final int[] blockSize = attributes.getBlockSize();

	final N5Writer n5Writer = N5GenericWriter(n5OutputPath);

	n5Writer.createDataset(outputDatasetName, dimensions, blockSize, DataType.UINT8, new GzipCompression());

	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	n5Writer.setAttribute(outputDatasetName, "pixelResolution", new IOHelper.PixelResolution(pixelResolution));
	n5Writer.setAttribute(outputDatasetName, "offset", IOHelper.getOffset(n5Reader, inputDatasetName));

	// n5Writer.setAttribute(outputDatasetName, "offset",
	// n5Reader.getAttribute(inputDatasetName, "offset", int[].class));

	double expansionInVoxels = expansionInNm / pixelResolution[0];
	int expansionInVoxelsCeil = (int) Math.ceil(expansionInVoxels);
	double expansionInVoxelsSquared = expansionInVoxels * expansionInVoxels;
	final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);

	rdd.foreach(blockInformation -> {
	    final long[][] gridBlock = blockInformation.gridBlock;
	    long[] offset = gridBlock[0];// new long[] {64,64,64};//gridBlock[0];////
	    long[] dimension = gridBlock[1];
	    long[] paddedOffset = new long[] { offset[0] - expansionInVoxelsCeil, offset[1] - expansionInVoxelsCeil,
		    offset[2] - expansionInVoxelsCeil };
	    long[] paddedDimension = new long[] { dimension[0] + 2 * expansionInVoxelsCeil,
		    dimension[1] + 2 * expansionInVoxelsCeil, dimension[2] + 2 * expansionInVoxelsCeil };
	    final N5Reader n5BlockReader = N5GenericReader(n5Path);

	    RandomAccessibleInterval<T> dataset = Views.offsetInterval(
		    Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, inputDatasetName)),
		    paddedOffset, paddedDimension);
	    RandomAccess<T> datasetRA = dataset.randomAccess();

	    final RandomAccessibleInterval<NativeBoolType> converted = Converters.convert(dataset, (a, b) -> {
		b.set(a.getIntegerLong() > thresholdIntensity);
	    }, new NativeBoolType());

	    ArrayImg<FloatType, FloatArray> distanceTransform = ArrayImgs.floats(paddedDimension);
	    DistanceTransform.binaryTransform(converted, distanceTransform, DISTANCE_TYPE.EUCLIDIAN);
	    RandomAccess<FloatType> distanceTransformRA = distanceTransform.randomAccess();

	    IntervalView<T> expanded = Views.offsetInterval(
		    Views.extendZero((RandomAccessibleInterval<T>) N5Utils.open(n5BlockReader, inputDatasetName)),
		    paddedOffset, paddedDimension);
	    RandomAccess<T> expandedRA = expanded.randomAccess();

	    for (int x = expansionInVoxelsCeil; x < paddedDimension[0] - expansionInVoxelsCeil; x++) {
		for (int y = expansionInVoxelsCeil; y < paddedDimension[1] - expansionInVoxelsCeil; y++) {
		    for (int z = expansionInVoxelsCeil; z < paddedDimension[2] - expansionInVoxelsCeil; z++) {
			int pos[] = new int[] { x, y, z };
			distanceTransformRA.setPosition(pos);
			float distanceSquared = distanceTransformRA.get().get();
			if (distanceSquared <= expansionInVoxelsSquared) {
			    expandedRA.setPosition(pos);
			    expandedRA.get().setInteger(255);
			}

		    }
		}
	    }

	    RandomAccessibleInterval<T> output = (RandomAccessibleInterval<T>) Views.offsetInterval(expanded,
		    new long[] { expansionInVoxelsCeil, expansionInVoxelsCeil, expansionInVoxelsCeil }, dimension);
	    final N5Writer n5BlockWriter = N5GenericWriter(n5OutputPath);
	    N5Utils.saveBlock(output, n5BlockWriter, outputDatasetName, gridBlock[2]);

	});

    }

    /**
     * Fill in all voxels within expanded region
     * 
     * @param expandedRA        Output expanded random access
     * @param objectID          Object ID of skeleton
     * @param pos               Position of skeleton voxel
     * @param paddedDimension   Padded dimensions
     * @param expansionInVoxels Expansion radius in voxels
     */
    public static <T extends IntegerType<T>> void fillInExpandedRegion(RandomAccess<T> expandedRA, long value,
	    int[] pos, long[] paddedDimension, int expansionInVoxels) {
	int expansionInVoxelsSquared = expansionInVoxels * expansionInVoxels;
	for (int x = pos[0] - expansionInVoxels; x <= pos[0] + expansionInVoxels; x++) {
	    for (int y = pos[1] - expansionInVoxels; y <= pos[1] + expansionInVoxels; y++) {
		for (int z = pos[2] - expansionInVoxels; z <= pos[2] + expansionInVoxels; z++) {
		    int dx = x - pos[0];
		    int dy = y - pos[1];
		    int dz = z - pos[2];
		    if ((dx * dx + dy * dy + dz * dz) <= expansionInVoxelsSquared) {
			if ((x >= 0 && y >= 0 && z >= 0)
				&& (x < paddedDimension[0] && y < paddedDimension[1] && z < paddedDimension[2])) {
			    expandedRA.setPosition(new int[] { x, y, z });
			    expandedRA.get().setInteger(value);
			}
		    }
		}

	    }
	}
    }

    public static final boolean allBlocksRequireDistanceTransform(String n5Path, String inputDatasetName,
	    double expansionInNm) throws IOException {
	final N5Reader n5Reader = N5GenericReader(n5Path);
	final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputDatasetName);
	final int[] blockSize = attributes.getBlockSize();
	double[] pixelResolution = IOHelper.getResolution(n5Reader, inputDatasetName);
	double expansionInVoxels = expansionInNm / pixelResolution[0];
	double expansionInVoxelsSquared = expansionInVoxels * expansionInVoxels;
	boolean needToDoDistanceTransform = Math.pow(blockSize[0], 2) + Math.pow(blockSize[1], 2)
		+ Math.pow(blockSize[2], 2) >= expansionInVoxelsSquared;
	return needToDoDistanceTransform;
    }

    /**
     * Expand skeleton for more visible meshes
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

	final SparkConf conf = new SparkConf().setAppName("SparkEpandDataset");

	// Get all organelles
	String[] organelles = { "" };
	if (options.getInputN5DatasetName() != null) {
	    organelles = options.getInputN5DatasetName().split(",");
	} else {
	    File file = new File(options.getInputN5Path());
	    organelles = file.list(new FilenameFilter() {
		@Override
		public boolean accept(File current, String name) {
		    return new File(current, name).isDirectory();
		}
	    });
	}

	System.out.println(Arrays.toString(organelles));
	String inputN5Path = options.getInputN5Path();
	String inputN5DatasetName = options.getInputN5DatasetName();
	String outputN5Path = options.getOutputN5Path();
	String outputN5DatasetName = options.getInputN5DatasetName() + options.getOutputN5DatasetSuffix();
	Integer thresholdIntensityCutoff = options.getThresholdIntensityCutoff();
	double expansionInNm = options.getExpansionInNm();
	boolean useFixedValue = options.getUseFixedValue();

	for (String currentOrganelle : organelles) {
	    // Create block information list
	    List<BlockInformation> blockInformationList = BlockInformation
		    .buildBlockInformationList(options.getInputN5Path(), currentOrganelle);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    if (useFixedValue && !allBlocksRequireDistanceTransform(inputN5Path, inputN5DatasetName, expansionInNm)) {
		// first only write out blocks that have objects
		// do distance transform and threshold (only required if long box diagonal is >
		// than distance). otherwise whole block is within distance
		// all blocks within certain distance are also all going to be fully thresholded
		ProcessingHelper.logMemory(blockInformationList.size() + " blocks for fillChunksContainingObjects");
		BlockInformationListsToRerun blockInformationListsToRerun = fillChunksContainingObjects(sc, inputN5Path,
			inputN5DatasetName, outputN5Path, outputN5DatasetName, thresholdIntensityCutoff, expansionInNm,
			blockInformationList);
		ProcessingHelper.logMemory(
			blockInformationListsToRerun.fillBlockInformationList.size() + " blocks for fillChunks");
		if (blockInformationListsToRerun.fillBlockInformationList.size() > 0) {
		    fillChunks(sc, inputN5Path, inputN5DatasetName, outputN5Path, outputN5DatasetName,
			    blockInformationListsToRerun.fillBlockInformationList);
		}
		blockInformationList = blockInformationListsToRerun.distanceTransformBlockInformationList;

	    }
	    ProcessingHelper.logMemory(blockInformationList.size() + " blocks for expandDataset");
	    if (blockInformationList.size() > 0) {
		/*for(BlockInformation blockInformation : blockInformationList) {
		    if(blockInformation.gridBlock[0][0]==128*6 && blockInformation.gridBlock[0][1]==128*1 && blockInformation.gridBlock[0][2]==128*7) {
			newList.add(blockInformation);
			System.out.println(Arrays.toString(blockInformation.gridBlock[0]));
		    }
		}*/
		expandDataset(sc, inputN5Path, inputN5DatasetName, outputN5Path, outputN5DatasetName,
			thresholdIntensityCutoff, expansionInNm, blockInformationList, useFixedValue);
	    }

	    sc.close();
	}

    }
}
