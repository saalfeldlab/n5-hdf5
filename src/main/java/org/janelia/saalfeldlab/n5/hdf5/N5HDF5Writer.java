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

import com.google.gson.GsonBuilder;

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
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.OpenDataSetCache.OpenDataSet;

import static hdf.hdf5lib.H5.H5Dget_space;
import static hdf.hdf5lib.H5.H5Dwrite;
import static hdf.hdf5lib.H5.H5Sclose;
import static hdf.hdf5lib.H5.H5Screate_simple;
import static hdf.hdf5lib.H5.H5Sselect_hyperslab;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static hdf.hdf5lib.HDF5Constants.H5S_SELECT_SET;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorderMultiplyToLong;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorderToLong;

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
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Writer} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param gsonBuilder
	 *            custom {@link GsonBuilder} to support custom attributes
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final IHDF5Writer writer,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws IOException {

		super(writer, overrideBlockSize, gsonBuilder, defaultBlockSize);
		this.writer = writer;
		setAttribute("/", VERSION_KEY, N5HDF5Reader.VERSION.toString());
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param writer
	 *            HDF5 writer
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Writer} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final IHDF5Writer writer,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) throws IOException {

		this(writer, overrideBlockSize, new GsonBuilder(), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param writer
	 *            HDF5 writer
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final IHDF5Writer writer,
			final int... defaultBlockSize) throws IOException {

		this(writer, false, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 writer
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Writer} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param gsonBuilder
	 *            custom {@link GsonBuilder} to support custom attributes
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws IOException {

		this(HDF5Factory.open(hdf5Path), overrideBlockSize, gsonBuilder, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 file name
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Writer} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) throws IOException {

		this(hdf5Path, overrideBlockSize, new GsonBuilder(), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Writer} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 file name
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final int... defaultBlockSize) throws IOException {

		this(hdf5Path, false, defaultBlockSize);
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
			final T attribute) {

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
			writer.string().setAttr(pathName, key, gson.toJson(attribute));
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

		final long[] hdf5DataBlockSize = reorderToLong(dataBlock.getSize());
		final long[] hdf5Offset = reorderMultiplyToLong(dataBlock.getGridPosition(), datasetAttributes.getBlockSize());

		try (OpenDataSet dataset = openDataSetCache.get(pathName)) {
			final long memorySpaceId = H5Screate_simple(hdf5DataBlockSize.length, hdf5DataBlockSize, null);
			final long fileSpaceId = H5Dget_space(dataset.dataSetId);
			H5Sselect_hyperslab(fileSpaceId, H5S_SELECT_SET, hdf5Offset, null, hdf5DataBlockSize, null);
			final long memTypeId = N5HDF5Util.memTypeId(datasetAttributes.getDataType());
			H5Dwrite(dataset.dataSetId, memTypeId, memorySpaceId, fileSpaceId, H5P_DEFAULT, dataBlock.getData());
			H5Sclose(fileSpaceId);
			H5Sclose(memorySpaceId);
		}
	}

	@Override
	public boolean deleteBlock(String pathName, final long... gridPosition) throws IOException {

		// deletion is not supported in HDF5, so the block is overwritten with zeroes instead

		if (pathName.equals(""))
			pathName = "/";

		final DatasetAttributes datasetAttributes = getDatasetAttributes(pathName);
		final DataType dataType = datasetAttributes.getDataType();
		switch (dataType) {
			case UINT8:
			case INT8:
			case UINT16:
			case INT16:
			case UINT32:
			case INT32:
			case UINT64:
			case INT64:
			case FLOAT32:
			case FLOAT64:
				final DataBlock<?> empty = dataType.createDataBlock(datasetAttributes.getBlockSize(), gridPosition);
				writeBlock(pathName, datasetAttributes, empty);
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean remove() {

		openDataSetCache.close();
		final File file = writer.file().getFile();
		writer.close();
		return file.delete();
	}

	@Override
	public boolean remove(String pathName) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		openDataSetCache.remove(pathName);
		writer.delete(pathName);
		return !writer.exists(pathName);
	}
}
