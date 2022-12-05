package org.janelia.cosem.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import inra.ijpb.binary.ChamferWeights3D;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.converter.Converters;
import net.imglib2.img.NativeImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.NativeBoolType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.algorithm.morphology.distance.EuclidianDistanceAnisotropic;

/**
 * Class to help with getting correctly padded distance transform
 */
public class CorrectlyPaddedGeodesicDistanceTransform {
    public long[] padding, paddedOffset, paddedDimension;
    public float [] weights;
    public RandomAccessibleInterval<FloatType> correctlyPaddedGeodesicDistanceTransform;

    /**
     * Get the correctly padded distance transform
     * 
     * @param <T>
     * @param mask    Source to do distance transform on
     * @param offset    Image offset
     * @param dimension Image dimension
     * @param threshold Threshold for binarization
     */
    public <T extends IntegerType<T> & NativeType<T>> CorrectlyPaddedGeodesicDistanceTransform(RandomAccessibleInterval<T> marker,
	    RandomAccessibleInterval<T> mask, long[] offset, long[] dimension, boolean normalizeMap) {

	weights = ChamferWeights3D.BORGEFORS.getFloatWeights();
	
	GeodesicDistanceTransform3DFloat<T> gdt = new GeodesicDistanceTransform3DFloat<T>(weights, normalizeMap);
	correctlyPaddedGeodesicDistanceTransform =  null;
	
	long[] maskDimensions = { 0, 0, 0 };
	mask.dimensions(maskDimensions);

	final long[] initialPadding = { 16, 16, 16 };

	padding = new long[3];
	paddedOffset = new long[3];
	paddedDimension = new long[3];
	final long[] minInside = new long[3];
	final long[] dimensionsInside = new long[3];

	long[] testPadding = initialPadding.clone();
	int shellPadding = 1;
	// Distance Transform
	A: for (boolean paddingIsTooSmall = true; paddingIsTooSmall; Arrays.setAll(testPadding,
		i -> testPadding[i] + initialPadding[i])) {

	    paddingIsTooSmall = false;
	    padding = testPadding.clone();
	    final long maxPadding = Arrays.stream(padding).max().getAsLong();

	    Arrays.setAll(paddedOffset, i -> offset[i] - padding[i]);
	    Arrays.setAll(paddedDimension, i -> dimension[i] + 2 * padding[i]);

	    final IntervalView<T> maskBlock = Views.offsetInterval(
		    Views.extendZero(mask), paddedOffset, paddedDimension);

	    final IntervalView<T> markerBlock = Views.offsetInterval(
		    Views.extendZero(marker), paddedOffset, paddedDimension);

	    /* make distance transform */

	    correctlyPaddedGeodesicDistanceTransform = gdt.geodesicDistanceMap(markerBlock.randomAccess(), maskBlock.randomAccess(), paddedDimension);


	    Arrays.setAll(minInside, i -> padding[i]);
	    Arrays.setAll(dimensionsInside, i -> dimension[i]);

	    final IntervalView<FloatType> insideBlock = Views
		    .offsetInterval(Views.extendZero(correctlyPaddedGeodesicDistanceTransform), minInside, dimensionsInside);

	    /* test whether distances at inside boundary are smaller than padding */
	    for (int d = 0; d < 3; ++d) {

		final IntervalView<FloatType> topSlice = Views.hyperSlice(insideBlock, d, 1);
		for (final FloatType t : topSlice)
		    if (t.get()!=gdt.backgroundValue && t.get() >= maxPadding - shellPadding) {
			// Subtract one from squareMaxPadding because we
			// want to ensure that if we have a shell in
			// later calculations for finding surface
			// points, we can access valid points
			// TODO: Remove shell padding?
			paddingIsTooSmall = true;
			continue A;
		    }

		final IntervalView<FloatType> botSlice = Views.hyperSlice(insideBlock, d, insideBlock.max(d));
		for (final FloatType t : botSlice)
		    if (t.get()!=gdt.backgroundValue && t.get() >= maxPadding - shellPadding) {
			paddingIsTooSmall = true;
			continue A;
		    }
	    }
	}
    }

}
