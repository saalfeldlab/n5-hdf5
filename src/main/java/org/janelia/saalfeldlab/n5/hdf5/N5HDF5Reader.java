/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.hdf5;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;

import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import net.imglib2.Interval;

/**
 * Best effort {@link N5Reader} implementation for HDF5 files.
 *
 * @author Stephan Saalfeld
 */
public class N5HDF5Reader implements N5Reader {

	protected final IHDF5Reader reader;

	protected final int[] defaultBlockSize;

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader HDF5 reader
	 * @param defaultBlockSize
	 * 				for all dimensions > defaultBlockSize.length, and for all
	 *              dimensions with defaultBlockSize[i] <= 0, the size of the
	 *              dataset will be used
	 */
	public N5HDF5Reader(final IHDF5Reader reader, final int... defaultBlockSize) {

		this.reader = reader;
		this.defaultBlockSize = defaultBlockSize;
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path HDF5 file name
	 * @param defaultBlockSize
	 * 				for all dimensions > defaultBlockSize.length, and for all
	 *              dimensions with defaultBlockSize[i] <= 0, the size of the
	 *              dataset will be used
	 */
	public N5HDF5Reader(final String hdf5Path, final int... defaultBlockSize) {

		this(HDF5Factory.openForReading(hdf5Path), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path HDF5 file name
	 */
	public N5HDF5Reader(final String hdf5Path) {

		this(HDF5Factory.openForReading(hdf5Path), 0);
	}

	@Override
	public boolean exists(String pathName) {

		if (pathName.equals("")) pathName = "/";

		return reader.exists(pathName);
	}

	@Override
	public String[] list(String pathName) {

		if (pathName.equals("")) pathName = "/";

		final List<String> members = reader.object().getGroupMembers(pathName);
		return members.toArray(new String[members.size()]);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getAttribute(String pathName, final String key, final Class<T> clazz) throws IOException {

		if (pathName.equals("")) pathName = "/";

		if (!reader.exists(pathName))
			return null;

		if (!reader.object().hasAttribute(pathName, key))
			return null;

		final HDF5DataTypeInformation attributeInfo = reader.object().getAttributeInformation(pathName, key);
		final Class<?> type = attributeInfo.tryGetJavaType();
		if (type.isAssignableFrom(long[].class))
			if (attributeInfo.isSigned())
				return (T) reader.int64().getArrayAttr(pathName, key);
			else
				return (T) reader.uint64().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(int[].class))
			if (attributeInfo.isSigned())
				return (T) reader.int32().getArrayAttr(pathName, key);
			else
				return (T) reader.uint32().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(short[].class))
			if (attributeInfo.isSigned())
				return (T) reader.int16().getArrayAttr(pathName, key);
			else
				return (T) reader.uint16().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(byte[].class)) {
			if (attributeInfo.isSigned())
				return (T) reader.int8().getArrayAttr(pathName, key);
			else
				return (T) reader.uint8().getArrayAttr(pathName, key);
		} else if (type.isAssignableFrom(double[].class))
			return (T) reader.float64().getArrayAttr(pathName, key);
		else if (type.isAssignableFrom(float[].class))
			return (T) reader.float32().getArrayAttr(pathName, key);
		else if (type.isAssignableFrom(String[].class))
			return (T) reader.string().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(long.class)) {
			if (attributeInfo.isSigned())
				return (T) new Long(reader.int64().getAttr(pathName, key));
			else
				return (T) new Long(reader.uint64().getAttr(pathName, key));
		} else if (type.isAssignableFrom(int.class)) {
			if (attributeInfo.isSigned())
				return (T) new Integer(reader.int32().getAttr(pathName, key));
			else
				return (T) new Integer(reader.uint32().getAttr(pathName, key));
		} else if (type.isAssignableFrom(short.class)) {
			if (attributeInfo.isSigned())
				return (T) new Short(reader.int16().getAttr(pathName, key));
			else
				return (T) new Short(reader.uint16().getAttr(pathName, key));
		} else if (type.isAssignableFrom(byte.class)) {
			if (attributeInfo.isSigned())
				return (T) new Byte(reader.int8().getAttr(pathName, key));
			else
				return (T) new Byte(reader.uint8().getAttr(pathName, key));
		} else if (type.isAssignableFrom(double.class))
			return (T) new Double(reader.float64().getAttr(pathName, key));
		else if (type.isAssignableFrom(float.class))
			return (T) new Double(reader.float32().getAttr(pathName, key));
		else if (type.isAssignableFrom(String.class))
			return (T) new String(reader.string().getAttr(pathName, key));

		System.err.println("Reading attributes of type " + attributeInfo + " not yet implemented.");
		return null;
	}

	protected static DataType getDataType(final HDF5DataSetInformation datasetInfo) {

		final HDF5DataTypeInformation typeInfo = datasetInfo.getTypeInformation();
		final Class<?> type = typeInfo.tryGetJavaType();
		if (type.isAssignableFrom(long.class)) {
			if (typeInfo.isSigned())
				return DataType.INT64;
			else
				return DataType.UINT64;
		} else if (type.isAssignableFrom(int.class)) {
			if (typeInfo.isSigned())
				return DataType.INT32;
			else
				return DataType.UINT32;
		} else if (type.isAssignableFrom(short.class)) {
			if (typeInfo.isSigned())
				return DataType.INT16;
			else
				return DataType.UINT16;
		} else if (type.isAssignableFrom(byte.class)) {
			if (typeInfo.isSigned())
				return DataType.INT8;
			else
				return DataType.UINT8;
		} else if (type.isAssignableFrom(double.class))
			return DataType.FLOAT64;
		else if (type.isAssignableFrom(float.class))
			return DataType.FLOAT32;

		System.err.println("Datasets of type " + typeInfo + " not yet implemented.");
		return null;
	}

	protected static void reorder(final long[] array) {

		long a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	protected static void reorder(final int[] array) {

		int a;
		final int max = array.length - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit
	 * into and {@link Interval} of given dimensions.  Fills long and int
	 * version of cropped block size.  Also calculates the grid raster position
	 * assuming that the offset divisible by block size without remainder.
	 *
	 * @param gridPosition
	 * @param dimensions
	 * @param blockSize
	 * @param croppedBlockSize
	 * @param offset
	 */
	protected static void cropBlockSize(
			final long[] gridPosition,
			final long[] dimensions,
			final int[] blockSize,
			final int[] croppedBlockSize,
			final long[] offset) {

		for (int d = 0; d < dimensions.length; ++d) {
			croppedBlockSize[d] = (int) Math.min(blockSize[d], dimensions[d] - offset[d]);
			offset[d] = gridPosition[d] * blockSize[d];
		}
	}

	protected int[] getBlockSize(final HDF5DataSetInformation datasetInfo) {

		final long[] dimensions = datasetInfo.getDimensions();
		int[] blockSize = datasetInfo.tryGetChunkSizes();
		if (blockSize != null)
			reorder(blockSize);
		else {
			blockSize = new int[dimensions.length];
			for (int i = 0; i < blockSize.length; ++i) {
				if (i >= defaultBlockSize.length || defaultBlockSize[i] <= 0)
					blockSize[i] = (int)dimensions[dimensions.length - i - 1];
				else
					blockSize[i] = defaultBlockSize[i];
			}
		}
		return blockSize;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(String pathName) {

		if (pathName.equals("")) pathName = "/";

		final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
		final long[] dimensions = datasetInfo.getDimensions();
		reorder(dimensions);
		int[] blockSize = datasetInfo.tryGetChunkSizes();
		if (blockSize != null)
			reorder(blockSize);
		else {
			blockSize = new int[dimensions.length];
			for (int i = 0; i < blockSize.length; ++i) {
				if (i >= defaultBlockSize.length || defaultBlockSize[i] <= 0)
					blockSize[i] = (int)dimensions[i];
				else
					blockSize[i] = defaultBlockSize[i];
			}
		}

		return new DatasetAttributes(
				dimensions,
				blockSize,
				getDataType(datasetInfo),
				CompressionType.RAW);
	}

	@Override
	public DataBlock<?> readBlock(
			String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		if (pathName.equals("")) pathName = "/";

		final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
		final long[] hdf5Dimensions = datasetInfo.getDimensions();
		int[] hdf5BlockSize = datasetInfo.tryGetChunkSizes();
		if (hdf5BlockSize == null) {
			hdf5BlockSize = new int[hdf5Dimensions.length];
			for (int i = hdf5BlockSize.length; i >= 0; --i) {
				final int j = hdf5BlockSize.length - 1 - i;
				if (i >= defaultBlockSize.length || defaultBlockSize[i] <= 0)
					hdf5BlockSize[j] = (int)hdf5Dimensions[j];
				else
					hdf5BlockSize[j] = defaultBlockSize[i];
			}
		}

		final long[] hdf5GridPosition = gridPosition.clone();
		reorder(hdf5GridPosition);
		final int[] hdf5CroppedBlockSize = new int[hdf5BlockSize.length];
		final long[] hdf5Offset = new long[hdf5GridPosition.length];
		cropBlockSize(
				hdf5GridPosition,
				hdf5Dimensions,
				hdf5BlockSize,
				hdf5CroppedBlockSize,
				hdf5Offset);

		final int[] croppedBlockSize = hdf5CroppedBlockSize.clone();
		reorder(croppedBlockSize);
		final DataBlock<?> dataBlock;
		switch (datasetAttributes.getDataType()) {
		case UINT8:
			final MDByteArray uint8Array = reader.uint8().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ByteArrayDataBlock(croppedBlockSize, gridPosition, uint8Array.getAsFlatArray());
			break;
		case INT8:
			final MDByteArray int8Array = reader.int8().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ByteArrayDataBlock(croppedBlockSize, gridPosition, int8Array.getAsFlatArray());
			break;
		case UINT16:
			final MDShortArray uint16Array = reader.uint16().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ShortArrayDataBlock(croppedBlockSize, gridPosition, uint16Array.getAsFlatArray());
			break;
		case INT16:
			final MDShortArray int16Array = reader.int16().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ShortArrayDataBlock(croppedBlockSize, gridPosition, int16Array.getAsFlatArray());
			break;
		case UINT32:
			final MDIntArray uint32Array = reader.uint32().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new IntArrayDataBlock(croppedBlockSize, gridPosition, uint32Array.getAsFlatArray());
			break;
		case INT32:
			final MDIntArray int32Array = reader.int32().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new IntArrayDataBlock(croppedBlockSize, gridPosition, int32Array.getAsFlatArray());
			break;
		case UINT64:
			final MDLongArray uint64Array = reader.uint64().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new LongArrayDataBlock(croppedBlockSize, gridPosition, uint64Array.getAsFlatArray());
			break;
		case INT64:
			final MDLongArray int64Array = reader.int64().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new LongArrayDataBlock(croppedBlockSize, gridPosition, int64Array.getAsFlatArray());
			break;
		case FLOAT32:
			final MDFloatArray float32Array = reader.float32().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new FloatArrayDataBlock(croppedBlockSize, gridPosition, float32Array.getAsFlatArray());
			break;
		case FLOAT64:
			final MDDoubleArray float64Array = reader.float64().readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new DoubleArrayDataBlock(croppedBlockSize, gridPosition, float64Array.getAsFlatArray());
			break;
		default:
			dataBlock = null;
		}

		return dataBlock;
	}

	@Override
	public boolean datasetExists(String pathName) {

		if (pathName.equals("")) pathName = "/";

		return reader.exists(pathName) && reader.object().isDataSet(pathName);
	}

	@Override
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		final String finalPathName = pathName.equals("") ? "/" : pathName;

		final HashMap<String, Class<?>> attributes = new HashMap<>();
		reader.object().getAttributeNames(finalPathName).forEach(
				attributeName -> attributes.put(attributeName, reader.object().getAttributeInformation(finalPathName, attributeName).tryGetJavaType()));
		return attributes;
	}

	public void close() {

		reader.close();
	}
}
