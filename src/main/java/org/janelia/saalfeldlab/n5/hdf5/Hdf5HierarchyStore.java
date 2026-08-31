package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import ch.systemsx.cisd.hdf5.HDF5FloatStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5IntStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;

import static org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import static org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import static org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorder;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.toDataType;

/**
 * {@code HierarchyStore} used  by {@link Hdf5Dialect}.
 * <p>
 * It exposes a best-effort read-only implementation of the {@code
 * HierarchyStore} interface that should work with {@code N5Dialect} for
 * reading. (The write-direction throws {@code UnsupportedOperationException}).
 * <p>
 * {@code Hdf5Dialect} uses the package-private methods to implement actual
 * (reading and writing) behavior.
 * <p>
 * {@link #readAttributesJson} creates a <em>virtual</em> "attributes.json" per
 * group/dataset. That view is lossy (a json number loses the exact HDF5 type).
 * Therefore, {@link Hdf5Dialect} instead uses the package-private methods
 * {@link #getAttribute}, {@link #setAttribute}, {@link #attributeClasses} which
 * retain the exact type for native HDF5 attributes.
 * <p>
 * Write operations require that the container was opened for writing.
 */
class Hdf5HierarchyStore implements HierarchyStore {

	/**
	 * Attribute name under which attributes that have no native HDF5
	 * representation are stored as json.
	 */
	private static final String N5_JSON_ROOT_KEY = "N5_JSON_ROOT";

	private final IHDF5Reader reader;
	private final IHDF5Writer writer; // may be null

	private final OpenDataSetCache openDataSetCache;

	private final int[] defaultBlockSize;

	private final boolean overrideBlockSize;

	Hdf5HierarchyStore(
			final IHDF5Reader reader,
			final OpenDataSetCache openDataSetCache,
			final int[] defaultBlockSize,
			final boolean overrideBlockSize) {

		this.reader = reader;
		this.writer = (reader instanceof IHDF5Writer) ? (IHDF5Writer) reader : null;

		this.openDataSetCache = openDataSetCache;
		this.defaultBlockSize = defaultBlockSize;
		this.overrideBlockSize = overrideBlockSize;
	}

	/**
	 * The HDF5 path of {@code path}: the normalized path, with the root
	 * directory mapped to {@code "/"}.
	 */
	static String hdf5Path(final N5DirectoryPath path) {

		final String normalPath = path.normalPath();
		return normalPath.isEmpty() ? "/" : normalPath;
	}

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ HierarchyStore: hierarchy                                             │
	// └───────────────────────────────────────────────────────────────────────┘

	@Override
	public boolean isDirectory(final N5DirectoryPath path) {

		return reader.exists(hdf5Path(path));
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * NB: A dataset has no children and lists as empty. (In N5 a dataset is a
	 * directory like any other, but an HDF5 dataset is not a group, so asking
	 * JHDF5 for its members would fail.)
	 */
	@Override
	public String[] listDirectories(final N5DirectoryPath path) throws N5IOException {

		final String pathName = hdf5Path(path);
		if (!reader.exists(pathName))
			throw new N5NoSuchKeyException("No such group or dataset: " + pathName);

		if (reader.object().isDataSet(pathName))
			return new String[0];

		try {
			return reader.object().getGroupMembers(pathName).toArray(new String[0]);
		} catch (final Exception e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public void createDirectories(final N5DirectoryPath path) {
		throw new UnsupportedOperationException("This HierarchyStore is a read-only view");
	}

	@Override
	public void removeDirectory(final N5DirectoryPath path) {
		throw new UnsupportedOperationException("This HierarchyStore is a read-only view");
	}

	@Override
	public void writeAttributesJson(final N5DirectoryPath parent, final String filename, final JsonElement attributes, final Gson gson) {
		throw new UnsupportedOperationException("This HierarchyStore is a read-only view");
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * NB: HDF5 has only one (virtual) "attributes.json" file per group/dataset.
	 * The returned view merges the native HDF5 attributes, the {@code
	 * N5_JSON_ROOT} tree, and (for datasets) the derived dataset attributes.
	 * Other {@code filename}s return {@code null}.
	 */
	@Override
	public JsonElement readAttributesJson(
			final N5DirectoryPath parent,
			final String filename,
			final Gson gson) throws N5IOException {

		if (!N5KeyValueReader.ATTRIBUTES_JSON.equals(filename))
			return null;

		return getAttribute(hdf5Path(parent), "/", JsonElement.class, gson);
	}









	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ Package-private implementation actually used by Hdf5Dialect           │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * Create an HDF5 group at {@code path}, and parent groups as necessary.
	 *
	 * @throws N5IOException
	 * 		if {@code path} exists but is not a group
	 */
	void createGroup(final N5DirectoryPath path) throws N5IOException {

		final String pathName = hdf5Path(path);
		if (reader.exists(pathName)) {
			if (!reader.isGroup(pathName))
				throw new N5IOException("\"" + pathName + "\" already exists and is not a group.");
		} else {
			writer.object().createGroup(pathName);
		}
	}

	/**
	 * Delete the group or dataset at {@code path}, and evict it from the {@code OpenDataSetCache}.
	 */
	void delete(final N5DirectoryPath path) throws N5IOException {

		final String hdf5Path = hdf5Path(path);
		openDataSetCache.remove(hdf5Path);
		writer.delete(hdf5Path);
	}

	boolean isDataSet(final String hdf5Path) {

		return reader.object().isDataSet(hdf5Path);
	}

	/**
	 * Derive N5 {@code DatasetAttributes} from the HDF5 dataset information.
	 * <p>
	 * Always reports {@code RawCompression} because HDF5 is responsible for the
	 * de/compression when we read/write {@code DataBlock}s.
	 *
	 * @return the dataset attributes, or {@code null} if {@code hdf5Path} is not a dataset
	 */
	DatasetAttributes datasetAttributes(final String hdf5Path) {

		if (!isDataSet(hdf5Path))
			return null;

		final HDF5DataSetInformation datasetInfo = reader.object().getDataSetInformation(hdf5Path);
		final long[] dimensions = reorder(datasetInfo.getDimensions());
		int[] blockSize = overrideBlockSize ? null : reorder(datasetInfo.tryGetChunkSizes());
		if (blockSize == null)
			blockSize = defaultBlockSize(dimensions);
		final DataType dataType = toDataType(datasetInfo.getTypeInformation());

		return new DatasetAttributes(dimensions, blockSize, dataType, new RawCompression());
	}

	private int[] defaultBlockSize(final long[] dimensions) {

		final int[] blockSize = Arrays.copyOf(defaultBlockSize, dimensions.length);
		for (int i = 0; i < blockSize.length; ++i)
			if (blockSize[i] <= 0)
				blockSize[i] = (int) dimensions[i];
		return blockSize;
	}

	void createDataset(
			final String hdf5Path,
			final DatasetAttributes datasetAttributes) throws N5Exception {

		final DataType dataType = datasetAttributes.getDataType();
		final Compression compression = datasetAttributes.getCompression();
		final HDF5IntStorageFeatures intCompression;
		final HDF5IntStorageFeatures uintCompression;
		final HDF5FloatStorageFeatures floatCompression;
		final HDF5GenericStorageFeatures stringCompression;
		if (compression instanceof RawCompression) {
			floatCompression = HDF5FloatStorageFeatures.FLOAT_NO_COMPRESSION;
			intCompression = HDF5IntStorageFeatures.INT_NO_COMPRESSION;
			uintCompression = HDF5IntStorageFeatures.INT_NO_COMPRESSION_UNSIGNED;
			stringCompression = HDF5GenericStorageFeatures.GENERIC_NO_COMPRESSION;
		} else {
			floatCompression = HDF5FloatStorageFeatures.FLOAT_SHUFFLE_DEFLATE;
			intCompression = HDF5IntStorageFeatures.INT_AUTO_SCALING_DEFLATE;
			uintCompression = HDF5IntStorageFeatures.INT_AUTO_SCALING_DEFLATE_UNSIGNED;
			stringCompression = HDF5GenericStorageFeatures.GENERIC_DEFLATE;
		}

		if (writer.exists(hdf5Path)) {
			openDataSetCache.remove(hdf5Path);
			writer.delete(hdf5Path);
		}

		final long[] hdf5Dimensions = reorder(datasetAttributes.getDimensions());
		final int[] hdf5BlockSize = reorder(datasetAttributes.getBlockSize());

		switch (dataType) {
		case UINT8:
			writer.uint8().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT16:
			writer.uint16().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT32:
			writer.uint32().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case UINT64:
			writer.uint64().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, uintCompression);
			break;
		case INT8:
			writer.int8().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT16:
			writer.int16().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT32:
			writer.int32().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case INT64:
			writer.int64().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, intCompression);
			break;
		case FLOAT32:
			writer.float32().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, floatCompression);
			break;
		case FLOAT64:
			writer.float64().createMDArray(hdf5Path, hdf5Dimensions, hdf5BlockSize, floatCompression);
			break;
		case STRING:
			writer.string().createMDArrayVL(hdf5Path, hdf5Dimensions, hdf5BlockSize, stringCompression);
			break;
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
	}

	/**
	 * Read the attribute {@code key} of the group or dataset at {@code
	 * pathName}, as {@code type}.
	 * <p>
	 * Native HDF5 attributes are read with their HDF5 type. Attributes that are
	 * not native HDF5 attributes are looked up in the {@code N5_JSON_ROOT} json
	 * tree. The key {@code "/"} returns the merged view of both (plus the
	 * derived dataset attributes, if {@code pathName} is a dataset).
	 * <p>
	 * Only a single-component {@code key} names a native attribute, under its
	 * unescaped name: {@code a\/b} reads the HDF5 attribute named {@code a/b},
	 * while {@code a/b} is the path {@code a} -> {@code b} in the json tree.
	 *
	 * @return the attribute, or {@code null} if the path or attribute does not exist
	 */
	<T> T getAttribute(
			final String pathName,
			final String key,
			final Type type,
			final Gson gson) throws N5Exception {

		final String normalizedAttrPath = N5URI.normalizeAttributePath(key).replaceFirst("^/", "");
		final String normalizedKey = normalizedAttrPath.isEmpty() ? "/" : normalizedAttrPath;

		if (!reader.exists(pathName))
			return null;

		final boolean isDataset = isDataSet(pathName);
		if (isDataset) {
			switch (normalizedKey) {
			case DatasetAttributes.DIMENSIONS_KEY:
				return coerceType(datasetAttributes(pathName).getDimensions(), type, gson);
			case DatasetAttributes.BLOCK_SIZE_KEY:
				return coerceType(datasetAttributes(pathName).getBlockSize(), type, gson);
			case DatasetAttributes.DATA_TYPE_KEY:
				return coerceType(datasetAttributes(pathName).getDataType(), type, gson);
			case DatasetAttributes.COMPRESSION_KEY:
				return coerceType(datasetAttributes(pathName).getCompression(), type, gson);
			}
		}

		// NB: only a single-component key can name a native attribute. A path
		// (e.g., "a/b", "a[0]") is looked up in the json tree even if a native
		// attribute of that name exists. To get the native attribute you would
		// use escaping (e.g., "a\/b", "a\[0]")
		final String nativeName = isAttributePath(normalizedKey) ? null : unescapeAttributeName(normalizedKey);

		if (normalizedKey.equals("/"))
			return getMergedAttributes(pathName, isDataset, type, gson);
		else if (nativeName != null && reader.object().hasAttribute(pathName, nativeName))
			return getNativeAttribute(pathName, nativeName, type, gson);
		else
			return getJsonAttribute(pathName, normalizedKey, type, gson);
	}

	/**
	 * For key {@code "/"}, {@link #getAttribute} merges all attributes of the
	 * group or dataset into one {@code JsonObject}.
	 * <p>
	 * The {@code N5_JSON_ROOT} json tree (if it exists, otherwise empty
	 * JsonObject) is the root. All native HDF5 attributes are json-encoded and
	 * added, as well as the derived dataset attributes (if {@code isDataset}).
	 */
	private <T> T getMergedAttributes(
			final String pathName,
			final boolean isDataset,
			final Type type,
			final Gson gson) throws N5Exception {

		final JsonElement jsonRoot = readJsonRoot(pathName, gson);
		final List<String> allAttributeNames = reader.object().getAttributeNames(pathName);
		final JsonElement attribute;
		if (allAttributeNames.size() > 1 || jsonRoot == null || isDataset) {
			final JsonObject attributeObj;
			if (jsonRoot != null && jsonRoot.isJsonObject()) {
				attributeObj = jsonRoot.getAsJsonObject();
			} else {
				attributeObj = new JsonObject();
				//TODO: do we really want to return the root if it's not an object? it exposes the `N5_JSON_ROOT_KEY`
				if (jsonRoot != null)
					attributeObj.add(N5_JSON_ROOT_KEY, jsonRoot);
			}
			for (final String attr : allAttributeNames) {
				if (attr.equals(N5_JSON_ROOT_KEY)) {
					continue;
				}
				attributeObj.add(attr, getNativeAttribute(pathName, attr, JsonElement.class, gson));
			}
			if (isDataset) {
				final DatasetAttributes datasetAttributes = datasetAttributes(pathName);
				attributeObj.add(DatasetAttributes.DIMENSIONS_KEY, gson.toJsonTree(datasetAttributes.getDimensions()));
				attributeObj.add(DatasetAttributes.BLOCK_SIZE_KEY, gson.toJsonTree(datasetAttributes.getBlockSize()));
				attributeObj.add(DatasetAttributes.DATA_TYPE_KEY, gson.toJsonTree(datasetAttributes.getDataType()));
				attributeObj.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(datasetAttributes.getCompression()));
			}
			attribute = attributeObj;
		} else {
			attribute = jsonRoot;
		}
		return parseJson(attribute, type, gson);
	}

	/**
	 * Read the attribute {@code normalizedKey} of the group or dataset at {@code
	 * pathName} from the {@code N5_JSON_ROOT} json tree, as {@code type}.
	 *
	 * @return the attribute, or {@code null} if it does not exist
	 */
	private <T> T getJsonAttribute(
			final String pathName,
			final String normalizedKey,
			final Type type,
			final Gson gson) throws N5Exception {

		return parseJson(GsonUtils.getAttribute(readJsonRoot(pathName, gson), normalizedKey), type, gson);
	}

	/**
	 * Read the json {@code attribute} as the requested {@code type}.
	 *
	 * @return the attribute, or {@code null} if {@code attribute} is {@code null}
	 *
	 * @throws N5ClassCastException
	 * 		if {@code attribute} cannot be read as {@code type}
	 */
	private static <T> T parseJson(final JsonElement attribute, final Type type, final Gson gson) {

		try {
			return GsonUtils.parseAttributeElement(attribute, gson, type);
		} catch (JsonParseException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}
	}

	/**
	 * Read the {@code N5_JSON_ROOT} json tree of the group or dataset at {@code
	 * pathName}, that is, all attributes that have no native HDF5
	 * representation.
	 *
	 * @return the json tree, or {@code null} if there is no {@code
	 * 		N5_JSON_ROOT} attribute
	 */
	private JsonElement readJsonRoot(final String pathName, final Gson gson) {

		if (!reader.object().hasAttribute(pathName, N5_JSON_ROOT_KEY))
			return null;

		return getNativeAttribute(pathName, N5_JSON_ROOT_KEY, JsonElement.class, gson);
	}

	/**
	 * Read the native HDF5 attribute {@code normalizedKey} of the group or
	 * dataset at {@code pathName}, as {@code type}. The attribute is read with
	 * its HDF5 type and then converted to {@code type}.
	 * <p>
	 * The attribute must exist, that is, {@code
	 * reader.object().hasAttribute(pathName, normalizedKey)}. (Otherwise, JHDF5
	 * throws {@code HDF5AttributeException}.)
	 *
	 * @throws N5ClassCastException
	 * 		if the attribute cannot be converted to {@code type}
	 */
	@SuppressWarnings("unchecked")
	private <T> T getNativeAttribute(
			final String pathName,
			final String normalizedKey,
			final Type requestedType,
			final Gson gson) throws N5Exception {

		// NB: GsonN5Reader.getAttribute(..., Class) canonicalizes array classes
		// (int[].class) into GenericArrayType, which the Class-based branches
		// below do not understand. Map those back to their raw class. Only
		// this method needs it: Gson handles GenericArrayType, so the json
		// and derived-attribute paths take the requested type as it comes.
		final Type type = requestedType instanceof GenericArrayType
				? TypeToken.get(requestedType).getRawType()
				: requestedType;

		final HDF5DataTypeInformation attributeInfo = reader.object().getAttributeInformation(pathName, normalizedKey);
		final Class<?> clazz = toAttributeClass(attributeInfo);
		final T hdf5Attribute;
		final boolean signed = attributeInfo.isSigned();
		if (clazz.isAssignableFrom(long[].class))
			hdf5Attribute = (T)(signed ? reader.int64() : reader.uint64()).getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(long[][].class))
			hdf5Attribute = (T)(signed ? reader.int64() : reader.uint64()).getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(int[].class))
			hdf5Attribute = (T)(signed ? reader.int32() : reader.uint32()).getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(int[][].class))
			hdf5Attribute = (T)(signed ? reader.int32() : reader.uint32()).getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(short[].class))
			hdf5Attribute = (T)(signed ? reader.int16() : reader.uint16()).getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(short[][].class))
			hdf5Attribute = (T)(signed ? reader.int16() : reader.uint16()).getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(byte[].class))
			hdf5Attribute = (T)(signed ? reader.int8() : reader.uint8()).getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(byte[][].class))
			hdf5Attribute = (T)(signed ? reader.int8() : reader.uint8()).getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(double[].class))
			hdf5Attribute = (T)reader.float64().getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(double[][].class))
			hdf5Attribute = (T)reader.float64().getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(float[].class))
			hdf5Attribute = (T)reader.float32().getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(float[][].class))
			hdf5Attribute = (T)reader.float32().getMatrixAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(String[].class))
			hdf5Attribute = (T)reader.string().getArrayAttr(pathName, normalizedKey);
		else if (clazz.isAssignableFrom(long.class))
			hdf5Attribute = (T)Long.valueOf((signed ? reader.int64() : reader.uint64()).getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(int.class))
			hdf5Attribute = (T)Integer.valueOf((signed ? reader.int32() : reader.uint32()).getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(short.class))
			hdf5Attribute = (T)Short.valueOf((signed ? reader.int16() : reader.uint16()).getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(byte.class))
			hdf5Attribute = (T)Byte.valueOf((signed ? reader.int8() : reader.uint8()).getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(double.class))
			hdf5Attribute = (T)Double.valueOf(reader.float64().getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(float.class))
			hdf5Attribute = (T)Float.valueOf(reader.float32().getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(boolean.class))
			hdf5Attribute = (T)Boolean.valueOf(reader.bool().getAttr(pathName, normalizedKey));
		else if (clazz.isAssignableFrom(String.class)) {
			final String attributeString = reader.string().getAttr(pathName, normalizedKey);
			if (TypeToken.get(type).getRawType().isInstance(attributeString)) {
				// the attribute already is what was requested (String, CharSequence, Object, ...)
				// Here the assumption is made that if the `Gson` object is configured to serializeNulls, and the retrieved attribute is the String `"null"`
				//	That it most likey was a quirk of a `null` value being serialized as the String `"null"`, and thus return `null`
				if (gson.serializeNulls() && attributeString.equals("null")) {
					return null;
				}
				return (T)attributeString;
			}
			try {
				if (type == JsonElement.class) {
					//TODO: See if this can be done better:
					//	If the `attributeString` is intended to be interpreted as a `String`, it needs to be wrapped with `"..."` quotes to make it a valid json string.
					//	Unfortunately it's not easy to know if the value is a json string or json structure until attempting to parse it.
					if (attributeString.isEmpty()) {
						return gson.fromJson("\"" + attributeString + "\"", type);
					}
					try {
						return gson.fromJson(attributeString, type);
					} catch (JsonSyntaxException e) {
						return gson.fromJson("\"" + attributeString + "\"", type);
					}
				}
				/* NB: gson takes any Type, so a generic one (a TypeToken) parses here too */
				return gson.fromJson(attributeString, type);
			} catch (JsonParseException | ClassCastException | NumberFormatException e) {
				throw new N5ClassCastException(e);
			}
		} else {
			hdf5Attribute = null;
		}

		if (type == JsonElement.class) {
			return (T)gson.toJsonTree(hdf5Attribute);
		} else if (type == String.class) {
			return (T)gson.toJson(hdf5Attribute);
		}

		return coerceType(hdf5Attribute, type, gson);
	}

	/**
	 * The Java type of the attribute described by {@code attributeInfo}.
	 * <p>
	 * This is {@link HDF5DataTypeInformation#tryGetJavaType()}, except that JHDF5
	 * reports a one-element array attribute with its <em>scalar</em> type ({@code
	 * long} for a {@code long[]} of length 1, {@code String} for a {@code String[]}
	 * of length 1). Reading those as scalars fails, so array attributes always map
	 * to the corresponding array class. (Rank-2 attributes already report their
	 * matrix type, whatever their extents.)
	 */
	private static Class<?> toAttributeClass(final HDF5DataTypeInformation attributeInfo) {

		final Class<?> javaType = attributeInfo.tryGetJavaType();
		if (attributeInfo.isArrayType() && javaType != null && !javaType.isArray())
			return Array.newInstance(javaType, 0).getClass();
		return javaType;
	}

	/**
	 * Coerce {@code value} to the requested {@code type}.
	 * <p>
	 * A value that already is of the requested type is returned as is, everything
	 * else is serialized to json and read back as {@code type}.
	 * <p>
	 * This is used to match behaviour of N5 and Zarr containers: {@link
	 * #getAttribute} should return native HDF5 attributes as the type that was
	 * requested (not the type they actually have).
	 *
	 * @throws N5ClassCastException
	 * 		if {@code value} cannot be read as {@code type}
	 */
	@SuppressWarnings("unchecked")
	private static <T> T coerceType(final Object value, final Type type, final Gson gson) {

		if (TypeToken.get(type).getRawType().isInstance(value))
			return (T)value;

		// NB: It is tempting to inline parseJson() below and replace all of its
		// usages with calls to coerceType().
		// However, this doesn't work, because of the above short-circuiting:
		// Requesting a JsonNull value as type Object.class would just return
		// the JsonNull (because Object.class.isInstance(jsonElement)==true)
		// while parseJson correctly returns null.

		return parseJson(gson.toJsonTree(value), type, gson);
	}

	/**
	 * List the attributes of the group or dataset at {@code pathName} and their
	 * classes, from the HDF5 attribute types.
	 * <p>
	 * String attributes will be parsed as JSON and classified as
	 * 1) An Object[] if it is a JsonArray
	 * 2) A  String   if it is a JsonPrimitive
	 * 3) An Object   if it is a JsonObject
	 */
	Map<String, Class<?>> attributeClasses(final String pathName) throws N5Exception {

		final HashMap<String, Class<?>> attributes = new HashMap<>();
		if (!reader.exists(pathName))
			return attributes;

		reader
				.object()
				.getAttributeNames(pathName)
				.forEach(
						attributeName -> {
							Class<?> clazz = toAttributeClass(
									reader.object().getAttributeInformation(pathName, attributeName));
							final boolean isN5JsonRoot = attributeName.equals(N5_JSON_ROOT_KEY);
							if (clazz.isAssignableFrom(String.class)) {
								//Attempt to parse the JSON
								try {
									String value = reader.string().getAttr(pathName, attributeName);
									JsonElement element = JsonParser.parseString(value);
									if (isN5JsonRoot && element.isJsonObject()) {
										/* Add the top level elements */
										for (final Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
											final JsonElement rootElement = entry.getValue();
											final Class<?> rootClass;
											if (rootElement.isJsonArray()) {
												rootClass = Object[].class;
											} else if (!rootElement.isJsonPrimitive()) {
												rootClass = Object.class;
											} else {
												rootClass = GsonUtils.classForJsonPrimitive(rootElement.getAsJsonPrimitive());
											}
											attributes.put(entry.getKey(), rootClass);
										}
									} else {
										if (element.isJsonArray())
											clazz = Object[].class;
										else if (!element.isJsonPrimitive())
											clazz = Object.class;
									}

									//A plain String is a JSON primitive
								} catch (JsonSyntaxException e) {
									//parsing fail, assume String.class
								}
							}
							if (!isN5JsonRoot) {
								attributes.put(attributeName, clazz);
							}
						}
				);
		return attributes;
	}

	/**
	 * Set the attribute {@code key} of the group or dataset at {@code pathName}.
	 * <p>
	 * Primitives, Strings, and 1D/2D arrays of those are written as native HDF5
	 * attributes. Everything else, as well as attribute <em>paths</em> and the
	 * key {@code "/"}, goes into the {@code N5_JSON_ROOT} json tree. Setting
	 * {@code "/"} removes all existing attributes.
	 * <p>
	 * A {@code key} that is a single (escaped) component names one attribute, and
	 * that unescaped name is what is written: {@code a\/b} sets the native HDF5
	 * attribute named {@code a/b}. See {@link #isAttributePath}.
	 */
	void setAttribute(
			final String pathName,
			final String key,
			final Object attribute,
			final Gson gson) throws N5Exception {

		final String normalizedAttrPath = N5URI.normalizeAttributePath(key).replaceFirst("^/", "");
		final String normalizedKey = normalizedAttrPath.isEmpty() ? "/" : normalizedAttrPath;

		final boolean isPath = isAttributePath(normalizedKey);
		final String nativeName = isPath ? null : unescapeAttributeName(normalizedKey);

		// Delete the existing attribute first because it may have a different type
		if (nativeName != null && writer.object().hasAttribute(pathName, nativeName)) {
			writer.object().deleteAttribute(pathName, nativeName);
		}

		// If setting the root attribute, we need to delete all existing attributes
		if (normalizedKey.equals("/")) {
			writer.object().getAttributeNames(pathName).forEach(it -> writer.object().deleteAttribute(pathName, it));
		}

		if (isPath) {
			setJsonAttribute(pathName, normalizedKey, attribute, gson);
			return;
		}

		if (!trySetNativeAttribute(pathName, nativeName, attribute))
			setJsonAttribute(pathName, normalizedKey, attribute, gson);
	}

	/**
	 * Whether {@code normalizedKey} is an attribute <em>path</em> rather than
	 * the name of a single attribute.
	 * <p>
	 * NB: The root key "/" (full attribute tree) counts as a path, too.
	 *
	 * @param normalizedKey
	 * 		attribute key with escaped special characters (unescaped "/", "["
	 * 		indicate that the key is a path/contains an array index)
	 */
	private static boolean isAttributePath(final String normalizedKey) {

		// The split is escape-aware, so "a\/b" is one component (the
		// name "a/b") while "a/b" is two. A single component that
		// is an array index ("[0]") indexes the root, and the root key
		// "/" is the whole document, so both are paths as well.
		return normalizedKey.equals("/")
				|| normalizedKey.split("(?<!\\\\)/").length > 1
				|| N5URI.ARRAY_INDEX.matcher(normalizedKey).matches();
	}

	/**
	 * The HDF5 attribute name denoted by the single-component attribute path
	 * {@code normalizedKey}: {@code a\/b} names the attribute {@code a/b}.
	 * <p>
	 * HDF5 attribute names are opaque strings, so {@code /} and {@code [} are
	 * legal in them and are unescaped here. Mirrors the unescaping of an object
	 * token in {@code N5URI.getAttributePathTokens}. NB: N5 attribute paths have
	 * no {@code \\} escape (yet), so a literal backslash passes through.
	 */
	private static String unescapeAttributeName(final String normalizedKey) {

		return normalizedKey.replaceAll("\\\\/", "/").replaceAll("\\\\\\[", "[");
	}

	/**
	 * Write {@code attribute} as a native HDF5 attribute named {@code key} on the
	 * group or dataset at {@code pathName}, if its Java type has a native HDF5
	 * representation.
	 * <p>
	 * {@code key} is the literal HDF5 attribute name, used verbatim -- callers
	 * pass {@link #unescapeAttributeName}, not the escaped N5 spelling.
	 *
	 * @return {@code true} if the attribute was written; {@code false} if {@code
	 * 		attribute} has no native HDF5 representation
	 */
	private boolean trySetNativeAttribute(final String pathName, final String key, final Object attribute) {

		if (attribute instanceof Boolean)
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
		else if (attribute instanceof byte[][])
			writer.int8().setMatrixAttr(pathName, key, (byte[][])attribute);
		else if (attribute instanceof short[])
			writer.int16().setArrayAttr(pathName, key, (short[])attribute);
		else if (attribute instanceof short[][])
			writer.int16().setMatrixAttr(pathName, key, (short[][])attribute);
		else if (attribute instanceof int[])
			writer.int32().setArrayAttr(pathName, key, (int[])attribute);
		else if (attribute instanceof int[][])
			writer.int32().setMatrixAttr(pathName, key, (int[][])attribute);
		else if (attribute instanceof long[])
			writer.int64().setArrayAttr(pathName, key, (long[])attribute);
		else if (attribute instanceof long[][])
			writer.int64().setMatrixAttr(pathName, key, (long[][])attribute);
		else if (attribute instanceof float[])
			writer.float32().setArrayAttr(pathName, key, (float[])attribute);
		else if (attribute instanceof float[][])
			writer.float32().setMatrixAttr(pathName, key, (float[][])attribute);
		else if (attribute instanceof double[])
			writer.float64().setArrayAttr(pathName, key, (double[])attribute);
		else if (attribute instanceof double[][])
			writer.float64().setMatrixAttr(pathName, key, (double[][])attribute);
		else if (attribute instanceof String[])
			writer.string().setArrayAttr(pathName, key, (String[])attribute);
		else
			return false;
		return true;
	}

	/**
	 * Write {@code attribute} into the {@code N5_JSON_ROOT} json tree of the
	 * group or dataset at {@code pathName}, under the attribute path {@code
	 * key}, creating the tree if it does not exist yet.
	 */
	private <T> void setJsonAttribute(final String pathName, final String key, final T attribute, final Gson gson) {

		/* Get the existing attributes, or create the root if not */
		JsonElement root = null;
		if (writer.object().hasAttribute(pathName, N5_JSON_ROOT_KEY)) {
			root = JsonParser.parseString(writer.string().getAttr(pathName, N5_JSON_ROOT_KEY));
		}

		//TODO How to handle writing top-level keys that have existing native keys (such as datasetAtributes)
		root = GsonUtils.insertAttribute(root, N5URI.normalizeAttributePath(key), attribute, gson );
		writer.string().setAttr(pathName, N5_JSON_ROOT_KEY, gson.toJson(root));
	}

	boolean removeAttribute(final String pathName, final String key, final Gson gson) throws N5Exception {

		if (!reader.exists(pathName)) {
			return false;
		}

		final String normalizedAttrPath = N5URI.normalizeAttributePath(key).replaceFirst("^/", "");
		final String normalizedKey = normalizedAttrPath.isEmpty() ? "/" : normalizedAttrPath;
		final String nativeName = isAttributePath(normalizedKey) ? null : unescapeAttributeName(normalizedKey);

		if (nativeName != null && writer.object().hasAttribute(pathName, nativeName)) {
			writer.object().deleteAttribute(pathName, nativeName);
			return true;
		}
		if (writer.object().hasAttribute(pathName, N5_JSON_ROOT_KEY)) {
			final JsonElement jsonRoot = getAttribute(pathName, N5_JSON_ROOT_KEY, JsonElement.class, gson);
			if (GsonUtils.removeAttribute(jsonRoot, N5URI.normalizeAttributePath(normalizedKey)) != null) {
				writer.string().setAttr(pathName, N5_JSON_ROOT_KEY, gson.toJson(jsonRoot));
				return true;
			}
		}
		return false;
	}

	<T> T removeAttribute(final String pathName, final String key, final Class<T> cls, final Gson gson) throws N5Exception {

		if (!reader.exists(pathName)) {
			return null;
		}

		final String normalizedAttrPath = N5URI.normalizeAttributePath(key).replaceFirst("^/", "");
		final String normalizedKey = (normalizedAttrPath.isEmpty() || normalizedAttrPath.equals(N5_JSON_ROOT_KEY)) ? "/" : normalizedAttrPath;
		final String nativeName = isAttributePath(normalizedKey) ? null : unescapeAttributeName(normalizedKey);

		final T removedAttribute = getAttribute(pathName, normalizedKey, cls, gson);
		if (removedAttribute != null) {
			if (nativeName != null && writer.object().hasAttribute(pathName, nativeName)) {
				writer.object().deleteAttribute(pathName, nativeName);
			}
			if (writer.object().hasAttribute(pathName, N5_JSON_ROOT_KEY)) {
				final JsonElement jsonRoot = getAttribute(pathName, N5_JSON_ROOT_KEY, JsonElement.class, gson);
				if (GsonUtils.removeAttribute(jsonRoot, N5URI.normalizeAttributePath(normalizedKey), cls, gson) != null) {
					writer.string().setAttr(pathName, N5_JSON_ROOT_KEY, gson.toJson(jsonRoot));
				}
			}
		}
		return removedAttribute;
	}
}
