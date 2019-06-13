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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;

import ch.systemsx.cisd.base.mdarray.MDByteArray;
import ch.systemsx.cisd.base.mdarray.MDDoubleArray;
import ch.systemsx.cisd.base.mdarray.MDFloatArray;
import ch.systemsx.cisd.base.mdarray.MDIntArray;
import ch.systemsx.cisd.base.mdarray.MDLongArray;
import ch.systemsx.cisd.base.mdarray.MDShortArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 * Best effort {@link N5Writer} implementation for HDF5 files.
 *
 * @author Stephan Saalfeld
 */
public class N5HDF5Writer extends N5HDF5Reader implements N5Writer {

	protected IHDF5Writer writer;

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param writer
	 *            HDF5 writer
	 * @param defaultBlockSize
	 *            for all dimensions > defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] <= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
	 */
	public N5HDF5Writer(final IHDF5Writer writer, final int... defaultBlockSize) throws IOException {

		super(writer, defaultBlockSize);
		this.writer = writer;
		setAttribute("/", VERSION_KEY, N5HDF5Reader.VERSION.toString());
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 file name
	 * @param defaultBlockSize
	 *            for all dimensions > defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] <= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
	 */
	public N5HDF5Writer(final String hdf5Path, final int... defaultBlockSize) throws IOException {

		this(HDF5Factory.open(hdf5Path), defaultBlockSize);
	}

	@Override
	public void createDataset(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		final DataType dataType = datasetAttributes.getDataType();
		final Compression compression = datasetAttributes.getCompression();
		final HDF5IntStorageFeatures intCompression;
		final HDF5IntStorageFeatures uintCompression;
		final HDF5FloatStorageFeatures floatCompression;
		if (compression instanceof RawCompression) {
			floatCompression = HDF5FloatStorageFeatures.FLOAT_NO_COMPRESSION;
			intCompression = HDF5IntStorageFeatures.INT_NO_COMPRESSION;
			uintCompression = HDF5IntStorageFeatures.INT_NO_COMPRESSION_UNSIGNED;
		} else {
			floatCompression = HDF5FloatStorageFeatures.FLOAT_SHUFFLE_DEFLATE;
			intCompression = HDF5IntStorageFeatures.INT_AUTO_SCALING_DEFLATE;
			uintCompression = HDF5IntStorageFeatures.INT_AUTO_SCALING_DEFLATE_UNSIGNED;
		}

		if (writer.exists(pathName))
			writer.delete(pathName);

		final long[] hdf5Dimensions = datasetAttributes.getDimensions().clone();
		reorder(hdf5Dimensions);
		final int[] hdf5BlockSize = datasetAttributes.getBlockSize().clone();
		reorder(hdf5BlockSize);

		switch (dataType) {
		case UINT8:
			writer.uint8().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT16:
			writer.uint16().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT32:
			writer.uint32().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT64:
			writer.uint64().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case INT8:
			writer.int8().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT16:
			writer.int16().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT32:
			writer.int32().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT64:
			writer.int64().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case FLOAT32:
			writer.float32().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, floatCompression);
			break;
		case FLOAT64:
			writer.float64().createMDArray(pathName, hdf5Dimensions, hdf5BlockSize, floatCompression);
		default:
			return;
		}
	}

	@Override
	public void createGroup(String pathName) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		if (writer.exists(pathName)) {
			if (!writer.isGroup(pathName))
				throw new IOException("Group " + pathName + " already exists and is not a group.");
		} else
			writer.object().createGroup(pathName);
	}

	@Override
	public <T> void setAttribute(
			String pathName,
			final String key,
			final T attribute) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		if (attribute == null)
			writer.object().deleteAttribute(pathName, key);
		else if (attribute instanceof Boolean)
			writer.bool().setAttr(pathName, key, (Boolean)attribute);
		else if (attribute instanceof Byte)
			writer.int8().setAttr(pathName, key, (Byte)attribute);
		else if (attribute instanceof Short)
			writer.int16().setAttr(pathName, key, (Short)attribute);
		else if (attribute instanceof Integer)
			writer.int32().setAttr(pathName, key, (Integer)attribute);
		else if (attribute instanceof Long)
			writer.int64().setAttr(pathName, key, (Long)attribute);
		else if (attribute instanceof Float)
			writer.float32().setAttr(pathName, key, (Float)attribute);
		else if (attribute instanceof Double)
			writer.float64().setAttr(pathName, key, (Double)attribute);
		else if (attribute instanceof String)
			writer.string().setAttr(pathName, key, (String)attribute);
		else if (attribute instanceof byte[])
			writer.int8().setArrayAttr(pathName, key, (byte[])attribute);
		else if (attribute instanceof short[])
			writer.int16().setArrayAttr(pathName, key, (short[])attribute);
		else if (attribute instanceof int[])
			writer.int32().setArrayAttr(pathName, key, (int[])attribute);
		else if (attribute instanceof long[])
			writer.int64().setArrayAttr(pathName, key, (long[])attribute);
		else if (attribute instanceof float[])
			writer.float32().setArrayAttr(pathName, key, (float[])attribute);
		else if (attribute instanceof double[])
			writer.float64().setArrayAttr(pathName, key, (double[])attribute);
		else if (attribute instanceof String[])
			writer.string().setArrayAttr(pathName, key, (String[])attribute);
		else
			throw new IOException("N5-HDF5: attributes of type " + attribute.getClass() + " not yet supported.");
	}

	@Override
	public void setAttributes(
			String pathName,
			final Map<String, ?> attributes) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		for (final Entry<String, ?> attribute : attributes.entrySet())
			setAttribute(pathName, attribute.getKey(), attribute.getValue());
	}

	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		throw new UnsupportedOperationException("HDF5 datasets cannot be reshaped.");
	}

	@Override
	public <T> void writeBlock(
			String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		final int[] hdf5DataBlockSize = dataBlock.getSize().clone();
		reorder(hdf5DataBlockSize);
		final int[] hdf5BlockSize = datasetAttributes.getBlockSize().clone();
		reorder(hdf5BlockSize);
		final long[] gridPosition = dataBlock.getGridPosition();
		final long[] hdf5Offset = new long[gridPosition.length];
		Arrays.setAll(hdf5Offset, i -> gridPosition[gridPosition.length - i - 1] * hdf5BlockSize[i]);
		switch (datasetAttributes.getDataType()) {
		case UINT8:
			final MDByteArray uint8TargetCell = new MDByteArray((byte[])dataBlock.getData(), hdf5DataBlockSize);
			writer.uint8().writeMDArrayBlockWithOffset(pathName, uint8TargetCell, hdf5Offset);
			break;
		case INT8:
			final MDByteArray int8TargetCell = new MDByteArray((byte[])dataBlock.getData(), hdf5DataBlockSize);
			writer.int8().writeMDArrayBlockWithOffset(pathName, int8TargetCell, hdf5Offset);
			break;
		case UINT16:
			final MDShortArray uint16TargetCell = new MDShortArray((short[])dataBlock.getData(), hdf5DataBlockSize);
			writer.uint16().writeMDArrayBlockWithOffset(pathName, uint16TargetCell, hdf5Offset);
			break;
		case INT16:
			final MDShortArray int16TargetCell = new MDShortArray((short[])dataBlock.getData(), hdf5DataBlockSize);
			writer.int16().writeMDArrayBlockWithOffset(pathName, int16TargetCell, hdf5Offset);
			break;
		case UINT32:
			final MDIntArray uint32TargetCell = new MDIntArray((int[])dataBlock.getData(), hdf5DataBlockSize);
			writer.uint32().writeMDArrayBlockWithOffset(pathName, uint32TargetCell, hdf5Offset);
			break;
		case INT32:
			final MDIntArray int32TargetCell = new MDIntArray((int[])dataBlock.getData(), hdf5DataBlockSize);
			writer.int32().writeMDArrayBlockWithOffset(pathName, int32TargetCell, hdf5Offset);
			break;
		case UINT64:
			final MDLongArray uint64TargetCell = new MDLongArray((long[])dataBlock.getData(), hdf5DataBlockSize);
			writer.uint64().writeMDArrayBlockWithOffset(pathName, uint64TargetCell, hdf5Offset);
			break;
		case INT64:
			final MDLongArray int64TargetCell = new MDLongArray((long[])dataBlock.getData(), hdf5DataBlockSize);
			writer.int64().writeMDArrayBlockWithOffset(pathName, int64TargetCell, hdf5Offset);
			break;
		case FLOAT32:
			final MDFloatArray float32TargetCell = new MDFloatArray((float[])dataBlock.getData(), hdf5DataBlockSize);
			writer.float32().writeMDArrayBlockWithOffset(pathName, float32TargetCell, hdf5Offset);
			break;
		case FLOAT64:
			final MDDoubleArray float64TargetCell = new MDDoubleArray((double[])dataBlock.getData(), hdf5DataBlockSize);
			writer.float64().writeMDArrayBlockWithOffset(pathName, float64TargetCell, hdf5Offset);
			break;
		}
	}

	@Override
	public boolean remove() {

		final File file = writer.file().getFile();
		writer.close();
		return file.delete();
	}

	@Override
	public boolean remove(String pathName) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		writer.delete(pathName);
		return !writer.exists(pathName);
	}
}
