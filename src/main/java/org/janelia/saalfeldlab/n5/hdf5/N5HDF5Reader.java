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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URL;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.scijava.util.VersionUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

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

/**
 * Best effort {@link N5Reader} implementation for HDF5 files.
 *
 * Attributes are not generally read as JSON but use HDF5 types. That means
 * that HDF5 files that were not generated with this library can be used
 * properly and correctly. Structured attributes for which no appropriate
 * HDF5 attribute type exists are parsed as JSON strings.
 *
 * @author Stephan Saalfeld
 * @author Philipp Hanslovsky
 */
public class N5HDF5Reader implements N5Reader, GsonAttributesParser, Closeable {

	protected final Gson gson;

	protected static final String N5_JSON_ROOT_KEY = "N5_JSON_ROOT";

	/**
	 * SemVer version of this N5-HDF5 spec.
	 */
	public static final Version VERSION =
			new Version(
					VersionUtils.getVersionFromPOM(
							N5HDF5Reader.class,
							"org.janelia.saalfeldlab",
							"n5-hdf5"));

	protected final IHDF5Reader reader;

	protected final int[] defaultBlockSize;

	protected boolean overrideBlockSize = false;

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader
	 *            HDF5 reader
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Reader} to use the
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
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws IOException {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		this.gson = gsonBuilder.create();

		this.reader = reader;
		final Version version = getVersion();
		if (!VERSION.isCompatible(version))
			throw new IOException("Incompatible N5-HDF5 version " + version + " (this is " + VERSION + ").");

		this.overrideBlockSize = overrideBlockSize;

		if (defaultBlockSize == null)
			this.defaultBlockSize = new int[0];
		else
			this.defaultBlockSize = defaultBlockSize;
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader
	 *            HDF5 reader
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Reader} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) throws IOException {

		this(reader, overrideBlockSize, new GsonBuilder(), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader
	 *            HDF5 reader
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final int... defaultBlockSize) throws IOException {

		this(reader, false, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 file name
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Reader} to use the
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
	public N5HDF5Reader(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws IOException {

		this(HDF5Factory.openForReading(hdf5Path), overrideBlockSize, gsonBuilder, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path
	 *            HDF5 file name
	 * @param overrideBlockSize
	 *            true if you want this {@link N5HDF5Reader} to use the
	 *            defaultBlockSize instead of the chunk-size for reading
	 *            datasets
	 * @param defaultBlockSize
	 *            for all dimensions &gt; defaultBlockSize.length, and for all
	 *            dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *            dataset will be used
	 * @throws IOException
     *            the exception
	 */
	public N5HDF5Reader(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) throws IOException {

		this(HDF5Factory.openForReading(hdf5Path), overrideBlockSize, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
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
	public N5HDF5Reader(
			final String hdf5Path,
			final int... defaultBlockSize) throws IOException {

		this(HDF5Factory.openForReading(hdf5Path), defaultBlockSize);
	}

	@Override
	public Gson getGson() {
		return gson;
	}

	@Override
	public boolean exists(String pathName) {

		if (pathName.equals(""))
			pathName = "/";

		return reader.exists(pathName);
	}

	@Override
	public String[] list(String pathName) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		try {
			final List<String> members = reader.object().getGroupMembers(pathName);
			return members.toArray(new String[members.size()]);
		} catch (final Exception e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getAttribute(String pathName, String key, final Class<T> clazz) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		if (key.equals("") || key.equals(N5_JSON_ROOT_KEY)) {
			key ="/";
		}

		if (!reader.exists(pathName))
			return null;
		
		if (datasetExists(pathName)) {

			if (key.equals("dimensions") && long[].class.isAssignableFrom(clazz)) {
				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				final long[] dimensions = datasetInfo.getDimensions();
				reorder(dimensions);
				return (T)dimensions;
			}
	
			if (key.equals("blockSize") && int[].class.isAssignableFrom(clazz)) {
				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				final long[] dimensions = datasetInfo.getDimensions();
				int[] blockSize = overrideBlockSize ? null : datasetInfo.tryGetChunkSizes();
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
				return (T)blockSize;
			}
	
			if (key.equals("dataType") && DataType.class.isAssignableFrom(clazz)) {
	
				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				return (T)getDataType(datasetInfo);
			}
	
			if (key.equals("compression") && Compression.class.isAssignableFrom(clazz))
				return (T)new RawCompression();
			
		}

		if (!reader.object().hasAttribute(pathName, key)) {
			/* Try to read the json if possible */
			if (!reader.object().hasAttribute( pathName, N5_JSON_ROOT_KEY )) {
				return null;
			}
			final JsonElement root = JsonParser.parseString(reader.string().getAttr( pathName, N5_JSON_ROOT_KEY ));
			return GsonAttributesParser.readAttribute( root,  N5URL.normalizeAttributePath( key), clazz, gson);
		}

		final HDF5DataTypeInformation attributeInfo = reader.object().getAttributeInformation(pathName, key);
		final Class<?> type = attributeInfo.tryGetJavaType();
		if (type.isAssignableFrom(long[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int64().getArrayAttr(pathName, key);
			else
				return (T)reader.uint64().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(int[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int32().getArrayAttr(pathName, key);
			else
				return (T)reader.uint32().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(short[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int16().getArrayAttr(pathName, key);
			else
				return (T)reader.uint16().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(byte[].class)) {
			if (attributeInfo.isSigned())
				return (T)reader.int8().getArrayAttr(pathName, key);
			else
				return (T)reader.uint8().getArrayAttr(pathName, key);
		} else if (type.isAssignableFrom(double[].class))
			return (T)reader.float64().getArrayAttr(pathName, key);
		else if (type.isAssignableFrom(float[].class))
			return (T)reader.float32().getArrayAttr(pathName, key);
		else if (type.isAssignableFrom(String[].class))
			return (T)reader.string().getArrayAttr(pathName, key);
		if (type.isAssignableFrom(long.class)) {
			if (attributeInfo.isSigned())
				return (T)new Long(reader.int64().getAttr(pathName, key));
			else
				return (T)new Long(reader.uint64().getAttr(pathName, key));
		} else if (type.isAssignableFrom(int.class)) {
			if (attributeInfo.isSigned())
				return (T)new Integer(reader.int32().getAttr(pathName, key));
			else
				return (T)new Integer(reader.uint32().getAttr(pathName, key));
		} else if (type.isAssignableFrom(short.class)) {
			if (attributeInfo.isSigned())
				return (T)new Short(reader.int16().getAttr(pathName, key));
			else
				return (T)new Short(reader.uint16().getAttr(pathName, key));
		} else if (type.isAssignableFrom(byte.class)) {
			if (attributeInfo.isSigned())
				return (T)new Byte(reader.int8().getAttr(pathName, key));
			else
				return (T)new Byte(reader.uint8().getAttr(pathName, key));
		} else if (type.isAssignableFrom(double.class))
			return (T)new Double(reader.float64().getAttr(pathName, key));
		else if (type.isAssignableFrom(float.class))
			return (T)new Float(reader.float32().getAttr(pathName, key));
		else if (type.isAssignableFrom(boolean.class))
			return (T)Boolean.valueOf(reader.bool().getAttr(pathName, key));
		else if (type.isAssignableFrom(String.class)) {
			final String attributeString = reader.string().getAttr(pathName, key);
			if (clazz.isAssignableFrom(String.class))
				return (T)attributeString;
			else
				return gson.fromJson(attributeString, clazz);
		}

		System.err.println("Reading attributes of type " + attributeInfo + " not yet implemented.");
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getAttribute(String pathName, final String key, final Type type) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		if (!reader.exists(pathName))
			return null;

		if (datasetExists(pathName)) {

			if (key.equals("dimensions") && type.getTypeName().equals(long[].class.getTypeName())) {
				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				final long[] dimensions = datasetInfo.getDimensions();
				reorder(dimensions);
				return (T)dimensions;
			}

			if (key.equals("blockSize") && type.getTypeName().equals(int[].class.getTypeName())) {
				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				final long[] dimensions = datasetInfo.getDimensions();
				int[] blockSize = overrideBlockSize ? null : datasetInfo.tryGetChunkSizes();
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
				return (T)blockSize;
			}

			if (key.equals("dataType") && type.getTypeName().equals(DataType.class.getTypeName())) {

				final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
				return (T)getDataType(datasetInfo);
			}

			if (key.equals("compression") && type.getTypeName().equals(Compression.class.getTypeName()))
				return (T)new RawCompression();
		}

		if (key.trim().isEmpty() || !reader.object().hasAttribute(pathName, key)) {
			/* Try to read the json if possible */
			if (!reader.object().hasAttribute( pathName, N5_JSON_ROOT_KEY )) {
				return null;
			}
			final JsonElement root = JsonParser.parseString(reader.string().getAttr( pathName, N5_JSON_ROOT_KEY ));
			return GsonAttributesParser.readAttribute( root,  N5URL.normalizeAttributePath( key), type, gson);
		}

		final HDF5DataTypeInformation attributeInfo = reader.object().getAttributeInformation(pathName, key);
		final Class<?> clazz = attributeInfo.tryGetJavaType();
		if (clazz.isAssignableFrom(long[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int64().getArrayAttr(pathName, key);
			else
				return (T)reader.uint64().getArrayAttr(pathName, key);
		if (clazz.isAssignableFrom(int[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int32().getArrayAttr(pathName, key);
			else
				return (T)reader.uint32().getArrayAttr(pathName, key);
		if (clazz.isAssignableFrom(short[].class))
			if (attributeInfo.isSigned())
				return (T)reader.int16().getArrayAttr(pathName, key);
			else
				return (T)reader.uint16().getArrayAttr(pathName, key);
		if (clazz.isAssignableFrom(byte[].class)) {
			if (attributeInfo.isSigned())
				return (T)reader.int8().getArrayAttr(pathName, key);
			else
				return (T)reader.uint8().getArrayAttr(pathName, key);
		} else if (clazz.isAssignableFrom(double[].class))
			return (T)reader.float64().getArrayAttr(pathName, key);
		else if (clazz.isAssignableFrom(float[].class))
			return (T)reader.float32().getArrayAttr(pathName, key);
		else if (clazz.isAssignableFrom(String[].class))
			return (T)reader.string().getArrayAttr(pathName, key);
		if (clazz.isAssignableFrom(long.class)) {
			if (attributeInfo.isSigned())
				return (T)new Long(reader.int64().getAttr(pathName, key));
			else
				return (T)new Long(reader.uint64().getAttr(pathName, key));
		} else if (clazz.isAssignableFrom(int.class)) {
			if (attributeInfo.isSigned())
				return (T)new Integer(reader.int32().getAttr(pathName, key));
			else
				return (T)new Integer(reader.uint32().getAttr(pathName, key));
		} else if (clazz.isAssignableFrom(short.class)) {
			if (attributeInfo.isSigned())
				return (T)new Short(reader.int16().getAttr(pathName, key));
			else
				return (T)new Short(reader.uint16().getAttr(pathName, key));
		} else if (clazz.isAssignableFrom(byte.class)) {
			if (attributeInfo.isSigned())
				return (T)new Byte(reader.int8().getAttr(pathName, key));
			else
				return (T)new Byte(reader.uint8().getAttr(pathName, key));
		} else if (clazz.isAssignableFrom(double.class))
			return (T)new Double(reader.float64().getAttr(pathName, key));
		else if (clazz.isAssignableFrom(float.class))
			return (T)new Float(reader.float32().getAttr(pathName, key));
		else if (clazz.isAssignableFrom(boolean.class))
			return (T)Boolean.valueOf(reader.bool().getAttr(pathName, key));
		else if (clazz.isAssignableFrom(String.class)) {
			final String attributeString = reader.string().getAttr(pathName, key);
			if (type.getTypeName().equals(String.class.getTypeName()))
				return (T)attributeString;
			else
				return gson.fromJson(attributeString, type);
		}

		System.err.println("Reading attributes of type " + attributeInfo + " not yet implemented.");
		return null;
	}

	/**
	 * Returns the attribute map for a group or dataset as {@link JsonElement}s.
	 * <p>
	 * String attributes are parsed as {@link JsonObject}s when they are valid json
	 * so that they may be converted to arbitrary objects. This means that strings
	 * that are valid json may not be recoverable from this method, use getAttribute
	 * instead.
	 * <p>
	 * Potential future work may instead store objects as a compound type rather than as strings.
	 *
	 * @param pathName the group or dataset path
	 * @return the attribute map
	 */
	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> attrs = new HashMap<>();
		final Map<String, Class<?>> attrClasses = listAttributes(pathName);
		for (final String k : attrClasses.keySet()) {
			if (
					attrClasses.get(k).equals(Object.class) ||
					attrClasses.get(k).equals(Object[].class)
			) {
				final String s = getAttribute(pathName, k, String.class);

				if (s.isEmpty()) {
					// check for empty string explicitly because it parses
					// as a null JsonObject without throwing an exception
					attrs.put(k, gson.toJsonTree(s));
				} else {
					JsonElement elem;
					try {
						elem = gson.fromJson(s, JsonObject.class);
					} catch (final JsonSyntaxException e) {
						elem = gson.toJsonTree(s);
					}
					/* if the root attr, add all it's top-level object names instead */
					if (k.equals(N5_JSON_ROOT_KEY) && elem.isJsonObject()) {
						elem.getAsJsonObject().entrySet().forEach((entry) -> attrs.put(entry.getKey(), entry.getValue()));
						continue;
					}
					attrs.put(k, elem);
				}

			} else
				attrs.put(k, gson.toJsonTree(getAttribute(pathName, k, attrClasses.get(k))));
		}

		if (datasetExists(pathName)) {
			final DatasetAttributes datasetAttributes = getDatasetAttributes(pathName);
			attrs.put("dimensions", gson.toJsonTree(datasetAttributes.getDimensions()));
			attrs.put("blockSize", gson.toJsonTree(datasetAttributes.getBlockSize()));
			attrs.put("dataType", gson.toJsonTree(datasetAttributes.getDataType() ));
			final JsonObject fakeRawCompression = new JsonObject();
			fakeRawCompression.addProperty("type", "raw");
			attrs.put("compression", fakeRawCompression);
		}

		return attrs;
	}

	@Override
	public JsonElement getAttributesJson( final String pathName ) throws IOException
	{
		final HashMap< String, JsonElement > attributesMap = getAttributes( pathName );
		final JsonObject attributes = new JsonObject();
		for ( final Map.Entry< String, JsonElement > keyAttribute: attributesMap.entrySet() )
		{
			final String key = keyAttribute.getKey();
			final JsonElement attribute = keyAttribute.getValue();
			attributes.add( key, attribute );
		}
		return attributes;
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
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit into
	 * an interval of given dimensions. Fills long and int version of cropped
	 * block size. Also calculates the grid raster position assuming that the
	 * offset is divisible by block size without remainder.
	 *
	 * @param gridPosition the coordinate of the block
	 * @param dimensions the dataset dimensions
	 * @param blockSize the block size
	 * @param croppedBlockSize the cropped block size to be filled
	 * @param offset the offset to be filled
	 */
	protected static void cropBlockSize(
			final long[] gridPosition,
			final long[] dimensions,
			final int[] blockSize,
			final int[] croppedBlockSize,
			final long[] offset) {

		for (int d = 0; d < dimensions.length; ++d) {
			offset[d] = gridPosition[d] * blockSize[d];
			croppedBlockSize[d] = (int)Math.min(blockSize[d], dimensions[d] - offset[d]);
		}
	}

	/**
	 * Always returns {@link CompressionType}#RAW because I could not yet find a
	 * meaningful way to get information about the compression of a dataset.
     *
     * @param pathName the group or dataset path
     * @return the DatasetAttributes
	 */
	@Override
	public DatasetAttributes getDatasetAttributes(String pathName) {

		if (pathName.equals(""))
			pathName = "/";
		
		if(!datasetExists(pathName))
			return null;

		final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(pathName);
		final long[] dimensions = datasetInfo.getDimensions();
		reorder(dimensions);
		int[] blockSize = overrideBlockSize ? null : datasetInfo.tryGetChunkSizes();

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
				new RawCompression());
	}

	@Override
	public DataBlock<?> readBlock(
			String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws IOException {

		if (pathName.equals(""))
			pathName = "/";

		final long[] hdf5Dimensions = datasetAttributes.getDimensions().clone();
		reorder(hdf5Dimensions);
		final int[] hdf5BlockSize = datasetAttributes.getBlockSize().clone();
		reorder(hdf5BlockSize);
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
			final MDByteArray uint8Array = reader
					.uint8()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ByteArrayDataBlock(croppedBlockSize, gridPosition, uint8Array.getAsFlatArray());
			break;
		case INT8:
			final MDByteArray int8Array = reader
					.int8()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ByteArrayDataBlock(croppedBlockSize, gridPosition, int8Array.getAsFlatArray());
			break;
		case UINT16:
			final MDShortArray uint16Array = reader
					.uint16()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ShortArrayDataBlock(croppedBlockSize, gridPosition, uint16Array.getAsFlatArray());
			break;
		case INT16:
			final MDShortArray int16Array = reader
					.int16()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new ShortArrayDataBlock(croppedBlockSize, gridPosition, int16Array.getAsFlatArray());
			break;
		case UINT32:
			final MDIntArray uint32Array = reader
					.uint32()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new IntArrayDataBlock(croppedBlockSize, gridPosition, uint32Array.getAsFlatArray());
			break;
		case INT32:
			final MDIntArray int32Array = reader
					.int32()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new IntArrayDataBlock(croppedBlockSize, gridPosition, int32Array.getAsFlatArray());
			break;
		case UINT64:
			final MDLongArray uint64Array = reader
					.uint64()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new LongArrayDataBlock(croppedBlockSize, gridPosition, uint64Array.getAsFlatArray());
			break;
		case INT64:
			final MDLongArray int64Array = reader
					.int64()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new LongArrayDataBlock(croppedBlockSize, gridPosition, int64Array.getAsFlatArray());
			break;
		case FLOAT32:
			final MDFloatArray float32Array = reader
					.float32()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new FloatArrayDataBlock(croppedBlockSize, gridPosition, float32Array.getAsFlatArray());
			break;
		case FLOAT64:
			final MDDoubleArray float64Array = reader
					.float64()
					.readMDArrayBlockWithOffset(pathName, hdf5CroppedBlockSize, hdf5Offset);
			dataBlock = new DoubleArrayDataBlock(croppedBlockSize, gridPosition, float64Array.getAsFlatArray());
			break;
		default:
			dataBlock = null;
		}

		return dataBlock;
	}

	@Override
	public boolean datasetExists(String pathName) {

		if (pathName.equals(""))
			pathName = "/";

		return reader.exists(pathName) && reader.object().isDataSet(pathName);
	}

	/**
	 * String attributes will be parsed as JSON and classified as
	 * 1) An Object[] if it is a JsonArray
	 * 2) A  String   if it is a JsonPrimitive
	 * 3) An Object   if it is a JsonObject
	 */
	@Override
	public Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		final String finalPathName = pathName.equals("") ? "/" : pathName;

		final HashMap<String, Class<?>> attributes = new HashMap<>();
		
		reader
				.object()
				.getAttributeNames(finalPathName)
				.forEach(
						attributeName -> {
							Class<?> clazz = reader
								.object()
								.getAttributeInformation(finalPathName, attributeName)
								.tryGetJavaType();
							if(clazz.isAssignableFrom(String.class)) {
								//Attempt to parse the JSON
								try {
									String value = reader.string().getAttr(finalPathName, attributeName);
									JsonElement element = JsonParser.parseString(value);
									if (attributeName.equals(N5_JSON_ROOT_KEY) && element.isJsonObject()) {
										/* Add the top level elements */
										for (final Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
											final JsonElement rootElement = entry.getValue();
											final Class<?> rootClass;
											if(rootElement.isJsonArray()) {
												rootClass = Object[].class;
											}
											else if(!rootElement.isJsonPrimitive()) {
												rootClass = Object.class;
											} else {
												rootClass = GsonAttributesParser.classForJsonPrimitive(rootElement.getAsJsonPrimitive());
											}
											attributes.put(entry.getKey(), rootClass);
										}
									} else {
										if(element.isJsonArray())
											clazz = Object[].class;
										else if(!element.isJsonPrimitive())
											clazz = Object.class;
									}

									//A plain String is a JSON primitive
								} catch(JsonSyntaxException e) {
									//parsing fail, assume String.class
								}
							}
							attributes
								.put(
										attributeName,
										clazz
								);
						}
				);
		return attributes;
	}

	@Override
	public void close() {

		reader.close();
	}

	/**
	 *
	 * @return file name of HDF5 file this reader is associated with
	 */
	public File getFilename() {

		return this.reader.file().getFile();
	}

	/**
	 *
	 * @return a copy of the default block size of this reader
	 */
	public int[] getDefaultBlockSizeCopy() {

		return defaultBlockSize.clone();
	}

	/**
	 *
	 * @return {@code true} if this reader overrides block size found in an
	 *         HDF5 dataset, {@code false} otherwise
	 */
	public boolean doesOverrideBlockSize() {

		return this.overrideBlockSize;
	}

	@Override
	public String toString() {

		return String.format("%s[file=%s]", getClass().getSimpleName(), reader.file().getFile());
	}

}
