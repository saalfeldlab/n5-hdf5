package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import hdf.hdf5lib.exceptions.HDF5Exception;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.ContainerDialect;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonN5Reader;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.hdf5.OpenDataSetCache.OpenDataSet;
import org.scijava.util.VersionUtils;

import static hdf.hdf5lib.H5.H5Dget_space;
import static hdf.hdf5lib.H5.H5Dread;
import static hdf.hdf5lib.H5.H5Sclose;
import static hdf.hdf5lib.H5.H5Screate_simple;
import static hdf.hdf5lib.H5.H5Sselect_hyperslab;
import static hdf.hdf5lib.HDF5Constants.H5S_SELECT_SET;
import static org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorderToLong;

/**
 * Best effort {@link N5Reader} implementation for HDF5 files.
 * <p>
 * Attributes are not generally read as JSON but use HDF5 types. That means
 * that HDF5 files that were not generated with this library can be used
 * properly and correctly. Structured attributes for which no appropriate
 * HDF5 attribute type exists are parsed as JSON strings.
 *
 * @author Stephan Saalfeld
 * @author Philipp Hanslovsky
 */
public class N5HDF5Reader implements GsonN5Reader, Closeable {

	/**
	 * SemVer version of this N5-HDF5 spec.
	 */
	public static final Version VERSION =
			new Version(
					VersionUtils.getVersionFromPOM(
							N5HDF5Reader.class,
							"org.janelia.saalfeldlab",
							"n5-hdf5"));

	private final IHDF5Reader reader;

	private final int[] defaultBlockSize;

	private final boolean overrideBlockSize;

	final OpenDataSetCache openDataSetCache;

	private final Hdf5Dialect containerDialect;

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader            HDF5 reader
	 * @param overrideBlockSize true if you want this {@link N5HDF5Reader} to use the
	 *                          defaultBlockSize instead of the chunk-size for reading
	 *                          datasets
	 * @param gsonBuilder       custom {@link GsonBuilder} to support custom attributes
	 * @param defaultBlockSize  for all dimensions &gt; defaultBlockSize.length, and for all
	 *                          dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                          dataset will be used
	 */
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws N5Exception {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		final Gson gson = gsonBuilder.create();

		this.reader = reader;
		this.overrideBlockSize = overrideBlockSize;

		if (defaultBlockSize == null)
			this.defaultBlockSize = new int[0];
		else
			this.defaultBlockSize = defaultBlockSize;

		this.openDataSetCache = new OpenDataSetCache(reader);
		final Hdf5HierarchyStore store = new Hdf5HierarchyStore(reader, openDataSetCache, this.defaultBlockSize, overrideBlockSize);
		this.containerDialect = new Hdf5Dialect(store, gson);

		// NB: getVersion goes through the containerDialect, so this has to happen after it is constructed.
		final Version version = getVersion();
		if (!VERSION.isCompatible(version)) {
			close();
			throw new N5Exception("Incompatible N5-HDF5 version " + version + " (this is " + VERSION + ").");
		}
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader            HDF5 reader
	 * @param overrideBlockSize true if you want this {@link N5HDF5Reader} to use the
	 *                          defaultBlockSize instead of the chunk-size for reading
	 *                          datasets
	 * @param defaultBlockSize  for all dimensions &gt; defaultBlockSize.length, and for all
	 *                          dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                          dataset will be used
	 */
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) {

		this(reader, overrideBlockSize, new GsonBuilder(), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param reader           HDF5 reader
	 * @param defaultBlockSize for all dimensions &gt; defaultBlockSize.length, and for all
	 *                         dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                         dataset will be used
	 */
	public N5HDF5Reader(
			final IHDF5Reader reader,
			final int... defaultBlockSize) {

		this(reader, false, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path          HDF5 file name
	 * @param overrideBlockSize true if you want this {@link N5HDF5Reader} to use the
	 *                          defaultBlockSize instead of the chunk-size for reading
	 *                          datasets
	 * @param gsonBuilder       custom {@link GsonBuilder} to support custom attributes
	 * @param defaultBlockSize  for all dimensions &gt; defaultBlockSize.length, and for all
	 *                          dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                          dataset will be used
	 */
	public N5HDF5Reader(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) {

		this(openHdf5Reader(hdf5Path), overrideBlockSize, gsonBuilder, defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path          HDF5 file name
	 * @param overrideBlockSize true if you want this {@link N5HDF5Reader} to use the
	 *                          defaultBlockSize instead of the chunk-size for reading
	 *                          datasets
	 * @param defaultBlockSize  for all dimensions &gt; defaultBlockSize.length, and for all
	 *                          dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                          dataset will be used
	 */
	public N5HDF5Reader(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) {

		this(hdf5Path, overrideBlockSize, new GsonBuilder(), defaultBlockSize);
	}

	/**
	 * Opens an {@link N5HDF5Reader} for a given HDF5 file.
	 *
	 * @param hdf5Path         HDF5 file name
	 * @param defaultBlockSize for all dimensions &gt; defaultBlockSize.length, and for all
	 *                         dimensions with defaultBlockSize[i] &lt;= 0, the size of the
	 *                         dataset will be used
	 */
	public N5HDF5Reader(
			final String hdf5Path,
			final int... defaultBlockSize)  {

		this(hdf5Path, false, defaultBlockSize);
	}

	@Override
	public ContainerDialect getContainerDialect() {

		return containerDialect;
	}

	@Override
	public String getAttributesKey() {

		throw new UnsupportedOperationException("HDF5 does not support separate attributes key");
	}

	private static IHDF5Reader openHdf5Reader(final String hdf5Path) {

		try {
			return HDF5Factory.openForReading(hdf5File(hdf5Path));
		} catch (final HDF5Exception e) {
			throw new N5IOException("Cannot open HDF5 Reader", new IOException(e));
		}
	}

	/**
	 * Resolves an HDF5 file location to a local {@link File}.
	 * <p>
	 * The location may be given either as a plain path, absolute or relative to
	 * the working directory, or as a {@code file:} URI. Anything that does not
	 * parse as a URI with a scheme is assumed to be a plain path.
	 *
	 * @param location
	 *            HDF5 file location
	 * @return the corresponding local file
	 */
	static File hdf5File(final String location) {

		URI uri;
		try {
			uri = URI.create(location);
			if (!uri.isAbsolute())
				uri = new File(location).toURI();
		} catch (final IllegalArgumentException e) {
			// location is not a valid URI, for example because it contains
			// spaces or Windows separators, so it can only be a path.
			uri = new File(location).toURI();
		}
		return Paths.get(uri.normalize()).toFile();
	}

	@Override
	public URI getURI() {
		return this.reader.file().getFile().toURI();
	}

	/**
	 * Crops the dimensions of a {@link DataBlock} at a given offset to fit into
	 * an interval of given dimensions. Fills long and int version of cropped
	 * block size. Also calculates the grid raster position assuming that the
	 * offset is divisible by block size without remainder.
	 *
	 * @param gridPosition     the coordinate of the block
	 * @param dimensions       the dataset dimensions
	 * @param blockSize        the block size
	 * @param croppedBlockSize the cropped block size to be filled
	 * @param offset           the offset to be filled
	 */
	private static void cropBlockSize(
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

	@Override
	public <T> DataBlock<T> readChunk(
			String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final String normalizedPathName = N5URI.normalizeGroupPath(pathName);
		pathName = normalizedPathName.isEmpty() ? "/" : normalizedPathName;

		final int n = datasetAttributes.getDimensions().length;
		final int[] croppedBlockSize = new int[n];
		final long[] hdf5Offset = new long[n];
		cropBlockSize(
				gridPosition,
				datasetAttributes.getDimensions(),
				datasetAttributes.getBlockSize(),
				croppedBlockSize,
				hdf5Offset);

		final long[] hdf5CroppedBlockSize = reorderToLong(croppedBlockSize);
		N5HDF5Util.reorderInPlace(hdf5Offset);

		if (datasetAttributes.getDataType() == DataType.STRING) {
			final int[] intHdf5CroppedBlockSize = Arrays.stream(hdf5CroppedBlockSize).mapToInt(i -> (int)i).toArray();
			MDArray<String> data = reader.string().readMDArrayBlockWithOffset(normalizedPathName, intHdf5CroppedBlockSize, hdf5Offset);
			return (DataBlock<T>)new StringDataBlock(croppedBlockSize, gridPosition, data.getAsFlatArray());
		}

		final DataType dataType = datasetAttributes.getDataType();
		final long memTypeId;
		try {
			memTypeId = N5HDF5Util.toH5T(dataType);
		} catch (IllegalArgumentException e) {
			return null;
		}
		final DataBlock<?> block = dataType.createDataBlock(croppedBlockSize, gridPosition.clone());

		try (OpenDataSet dataset = openDataSetCache.get(pathName)) {
			final long memorySpaceId = H5Screate_simple(hdf5CroppedBlockSize.length, hdf5CroppedBlockSize, null);
			final long fileSpaceId = H5Dget_space(dataset.dataSetId);
			H5Sselect_hyperslab(fileSpaceId, H5S_SELECT_SET, hdf5Offset, null, hdf5CroppedBlockSize, null);
			H5Dread(dataset.dataSetId, memTypeId, memorySpaceId, fileSpaceId, openDataSetCache.numericConversionXferPropertyListID, block.getData());
			H5Sclose(fileSpaceId);
			H5Sclose(memorySpaceId);
		}
		return (DataBlock<T>)block;
	}

	@Override
	public <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		// HDF5 does not support sharding, so readBlock is always equivalent to readChunk
		return readChunk(pathName, datasetAttributes, gridPosition);
	}

	@Override
	public boolean blockExists(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) throws N5Exception {

		return true;
	}

	@Override
	public void close() {

		openDataSetCache.close();
		reader.close();
	}

	/**
	 * @return file name of HDF5 file this reader is associated with
	 */
	public File getFilename() {

		return this.reader.file().getFile();
	}

	/**
	 * @return a copy of the default block size of this reader
	 */
	public int[] getDefaultBlockSizeCopy() {

		return defaultBlockSize.clone();
	}

	/**
	 * @return {@code true} if this reader overrides block size found in an
	 * HDF5 dataset, {@code false} otherwise
	 */
	public boolean doesOverrideBlockSize() {

		return this.overrideBlockSize;
	}

	@Override
	public String toString() {

		return String.format("%s[file=%s]", getClass().getSimpleName(), reader.file().getFile());
	}

}
