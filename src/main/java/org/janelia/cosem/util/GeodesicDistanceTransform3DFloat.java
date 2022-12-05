package org.janelia.cosem.util;

/*-
 * #%L
 * Mathematical morphology library and plugins for ImageJ/Fiji.
 * %%
 * Copyright (C) 2014 - 2017 INRA.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */

import ij.ImageStack;
import inra.ijpb.algo.AlgoStub;
import inra.ijpb.binary.ChamferWeights3D;
import inra.ijpb.data.image.Image3D;
import inra.ijpb.data.image.Images3D;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * Computation of geodesic distance transform for 3D images, using floating point computation.
 * 
 * @author dlegland
 * @param <T>
 *
 */
public class GeodesicDistanceTransform3DFloat<T extends IntegerType<T> & NativeType<T>>
{
	private final static int DEFAULT_MASK_LABEL = 255;

	// ==================================================
	// Class variables
	
	float[] weights;
	
	/**
	 * Flag for dividing final distance map by the value first weight. 
	 * This results in distance map values closer to euclidean, but with non integer values. 
	 */
	boolean normalizeMap = true;
	
	/** 
	 * The value assigned to result pixels that do not belong to the mask. 
	 * Default is Float.MAX_VALUE.
	 */
	float backgroundValue = Float.POSITIVE_INFINITY;
	
	int maskLabel = DEFAULT_MASK_LABEL;

	RandomAccess<T> maskProc;
	RandomAccess<FloatType> result;
	
	int sizeX;
	int sizeY;
	int sizeZ;

	boolean modif;

	
	// ==================================================
	// Constructors
	
	public GeodesicDistanceTransform3DFloat(float[] weights)
	{
		this.weights = weights;
	}

	public GeodesicDistanceTransform3DFloat(float[] weights, boolean normalizeMap)
	{
		this.weights = weights;
		this.normalizeMap = normalizeMap;
	}

	public GeodesicDistanceTransform3DFloat(ChamferWeights3D weights, boolean normalizeMap)
	{
		this.weights = weights.getFloatWeights();
		this.normalizeMap = normalizeMap;
	}


	// ==================================================
	// Methods
	
	/* (non-Javadoc)
	 * @see inra.ijpb.binary.geodesic.GeodesicDistanceTransform3D#geodesicDistanceMap(ij.ImageStack, ij.ImageStack)
	 */
	public  RandomAccessibleInterval<FloatType> geodesicDistanceMap(RandomAccess<T> marker, RandomAccess<T> mask, long [] dimension)
	{
		this.maskProc = mask;
		this.sizeX = (int) dimension[0] ;
		this.sizeY = (int) dimension[1];
		this.sizeZ = (int) dimension[2];
		
		// create new empty image, and fill it with black
		RandomAccessibleInterval<FloatType> resultStack = Views.offsetInterval(ArrayImgs.floats(dimension),new long[]{0,0,0}, dimension);
		this.result = resultStack.randomAccess();
			
		// initialize empty image with either 0 (foreground) or Inf (background)
		for (int z = 0; z < sizeZ; z++)
		{
			for (int y = 0; y < sizeY; y++)
			{
				for (int x = 0; x < sizeX; x++)
				{
				    	marker.setPosition(new int[] {x,y,z});
					if (marker.get().getIntegerLong() == 0)
					{
					    	result.setPosition(new int[] {x,y,z});
						result.get().set(backgroundValue);
					}
				}
			}
		}
		
		// Iterate forward and backward passes until no more modification occur
		int iter = 0;
		do 
		{
			modif = false;

			// forward iteration
			forwardIteration();

			// backward iteration
			backwardIteration();

			// Iterate while pixels have been modified
			iter++;
		} while (modif);

		// Normalize values by the first weight value
		if (this.normalizeMap) 
		{
			for (int z = 0; z < sizeZ; z++)
			{
				for (int y = 0; y < sizeY; y++)
				{
					for (int x = 0; x < sizeX; x++)
					{
					    	result.setPosition(new int[] {x,y,z});
						float val = result.get().get() / weights[0];
						result.get().set(val);
					}
				}
			}
		}

//		// Compute max value within the mask
//		fireStatusChanged(this, "Normalize display"); 
//		float maxVal = 0;
//		for (int z = 0; z < sizeZ; z++) 
//		{
//			for (int y = 0; y < sizeY; y++) 
//			{
//				for (int x = 0; x < sizeX; x++)
//				{
//					float val = (float) result.getValue(x, y, z);
//					if (Float.isFinite(val))
//					{
//						maxVal = Math.max(maxVal, val);
//					}
//				}
//			}
//		}
		
//		// update and return resulting Image processor
//		result.setFloatArray(array);
//		result.setMinAndMax(0, maxVal);
//		// Forces the display to non-inverted LUT
//		if (result.isInvertedLut())
//			result.invertLut();
//		return result;

		return resultStack;
	}

	private void forwardIteration()
	{
		int[][] shifts = new int[][]{
				{-1, -1, -1},
				{ 0, -1, -1},
				{+1, -1, -1},
				{-1,  0, -1},
				{ 0,  0, -1},
				{+1,  0, -1},
				{-1, +1, -1},
				{ 0, +1, -1},
				{+1, +1, -1},
				{-1, -1,  0},
				{ 0, -1,  0},
				{+1, -1,  0},
				{-1,  0,  0},
		};
		
		double[] shiftWeights = new double[]{
				weights[2], weights[1], weights[2], 
				weights[1], weights[0], weights[1], 
				weights[2], weights[1], weights[2], 
				weights[1], weights[0], weights[1], 
				weights[0]
		};
		
		for (int z = 0; z < sizeZ; z++)
		{
			for (int y = 0; y < sizeY; y++)
			{
				for (int x = 0; x < sizeX; x++)
				{
					// process only voxels within the mask
				    	maskProc.setPosition(new int[] {x,y,z});
					if (maskProc.get().getIntegerLong() == 0)//DGA commenting this out since we may have different labels != maskLabel)
					{
						continue;
					}
					
					result.setPosition(new int[] {x,y,z});
					double value = result.get().get();
					double ref = value;
					
					// find minimal value in forward neighborhood
					for (int i = 0; i < shifts.length; i++)
					{
						int[] shift = shifts[i];
						int x2 = x + shift[0];
						int y2 = y + shift[1];
						int z2 = z + shift[2];
						
						if (x2 < 0 || x2 >= sizeX)
							continue;
						if (y2 < 0 || y2 >= sizeY)
							continue;
						if (z2 < 0 || z2 >= sizeZ)
							continue;
						
						result.setPosition(new int[] {x2,y2,z2});
						double newVal = result.get().get() + shiftWeights[i];
						value = Math.min(value, newVal);
					}
					
					if (value < ref)
					{
						modif = true;
						result.setPosition(new int[] {x,y,z});
						result.get().set((float) value);
					}
				}
			}
		}
		
	}

	private void backwardIteration()
	{
		int[][] shifts = new int[][]{
				{+1, +1, +1},
				{ 0, +1, +1},
				{-1, +1, +1},
				{+1,  0, +1},
				{ 0,  0, +1},
				{-1,  0, +1},
				{+1, -1, +1},
				{ 0, -1, +1},
				{-1, -1, +1},
				{+1, +1,  0},
				{ 0, +1,  0},
				{-1, +1,  0},
				{+1,  0,  0},
		};
		
		double[] shiftWeights = new double[]{
				weights[2], weights[1], weights[2], 
				weights[1], weights[0], weights[1], 
				weights[2], weights[1], weights[2], 
				weights[1], weights[0], weights[1], 
				weights[0]
		};
		
		for (int z = sizeZ-1; z >= 0; z--)
		{
			for (int y = sizeY - 1; y >= 0; y--)
			{
				for (int x = sizeX - 1; x >= 0; x--)
				{
					// process only voxels within the mask
				    	maskProc.setPosition(new int[] {x,y,z});
					if (maskProc.get().getIntegerLong() == 0) //DGA commenting this out since we may have different labels != maskLabel)
					{
						continue;
					}
					
					result.setPosition(new int[] {x,y,z});
					double value = result.get().get();
					double ref = value;
					
					// find minimal value in backward neighborhood
					for (int i = 0; i < shifts.length; i++)
					{
						int[] shift = shifts[i];
						int x2 = x + shift[0];
						int y2 = y + shift[1];
						int z2 = z + shift[2];
						
						if (x2 < 0 || x2 >= sizeX)
							continue;
						if (y2 < 0 || y2 >= sizeY)
							continue;
						if (z2 < 0 || z2 >= sizeZ)
							continue;
						
						result.setPosition(new int[] {x2,y2,z2});
						double newVal = result.get().get() + shiftWeights[i];
						value = Math.min(value, newVal);
					}
					
					if (value < ref)
					{
						modif = true;
						result.setPosition(new int[] {x,y,z});
						result.get().set((float) value);
					}
				}
			}
		}	
		
	}
}
