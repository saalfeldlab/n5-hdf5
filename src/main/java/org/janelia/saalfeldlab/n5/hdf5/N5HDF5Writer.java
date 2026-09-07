package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.base.mdarray.MDArray;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.google.gson.GsonBuilder;

import hdf.hdf5lib.exceptions.HDF5Exception;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonN5Writer;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.OpenDataSetCache.OpenDataSet;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.shard.Region;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static hdf.hdf5lib.H5.H5Dget_space;
import static hdf.hdf5lib.H5.H5Dwrite;
import static hdf.hdf5lib.H5.H5Sclose;
import static hdf.hdf5lib.H5.H5Screate_simple;
import static hdf.hdf5lib.H5.H5Sselect_hyperslab;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static hdf.hdf5lib.HDF5Constants.H5S_SELECT_SET;
import static org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorderMultiplyToLong;
import static org.janelia.saalfeldlab.n5.hdf5.N5HDF5Util.reorderToLong;

/**
 * Best effort {@link N5Writer} implementation for HDF5 files.
 *
 * @author Stephan Saalfeld
 */
public class N5HDF5Writer extends N5HDF5Reader implements GsonN5Writer {

	final IHDF5Writer writer;

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
	 * @throws N5Exception
     *            the exception
	 */
	public N5HDF5Writer(
			final IHDF5Writer writer,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) throws N5Exception {

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
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final GsonBuilder gsonBuilder,
			final int... defaultBlockSize) {

		this(openHdf5Writer(hdf5Path), overrideBlockSize, gsonBuilder, defaultBlockSize);
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
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final boolean overrideBlockSize,
			final int... defaultBlockSize) {

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
	 */
	public N5HDF5Writer(
			final String hdf5Path,
			final int... defaultBlockSize) {

		this(hdf5Path, false, defaultBlockSize);
	}

	@Override
	public <T> void writeChunk(
			String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		final String normalizedPathName = N5URI.normalizeGroupPath(pathName);
		pathName = normalizedPathName.isEmpty() ? "/" : normalizedPathName;

		final long[] hdf5DataBlockSize = reorderToLong(dataBlock.getSize());
		final long[] hdf5Offset = reorderMultiplyToLong(dataBlock.getGridPosition(), datasetAttributes.getBlockSize());

		if (datasetAttributes.getDataType() == DataType.STRING) {
			MDArray<String> arr = new MDArray<>((String[]) dataBlock.getData(), hdf5DataBlockSize);
			writer.string().writeMDArrayBlockWithOffset(pathName, arr, hdf5Offset);
			return;
		}

		try (OpenDataSet dataset = openDataSetCache.get(pathName)) {
			final long memorySpaceId = H5Screate_simple(hdf5DataBlockSize.length, hdf5DataBlockSize, null);
			final long fileSpaceId = H5Dget_space(dataset.dataSetId);
			H5Sselect_hyperslab(fileSpaceId, H5S_SELECT_SET, hdf5Offset, null, hdf5DataBlockSize, null);
			final long memTypeId = N5HDF5Util.toH5T(datasetAttributes.getDataType());
			H5Dwrite(dataset.dataSetId, memTypeId, memorySpaceId, fileSpaceId, H5P_DEFAULT, dataBlock.getData());
			H5Sclose(fileSpaceId);
			H5Sclose(memorySpaceId);
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		// HDF5 does not support sharding, so writeBlock is always equivalent to writeChunk
		writeChunk(pathName, datasetAttributes, dataBlock);
	}

	@Override
	public <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> dataBlocks,
			final boolean writeFully) throws N5Exception {

		final NestedGrid grid = datasetAttributes.getNestedBlockGrid();
		final Region region = new Region(min, size, grid);
		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			final NestedPosition pos = grid.nestedPosition(key, 0); // HDF5 is never nested, get level 0
			final long[] gridPosition = pos.absolute(0);
			final DataBlock<T> existingDataBlock = writeFully || region.fullyContains(pos)
					? null
					: readBlock(datasetPath, datasetAttributes, gridPosition);
			final DataBlock<T> dataBlock = dataBlocks.get(gridPosition, existingDataBlock);
			// null blocks may be provided when they contain only the fill value
			// and only non-empty blocks should be written, for example
			if (dataBlock == null) {
				deleteBlock(datasetPath, datasetAttributes, gridPosition);
			} else {
				writeBlock(datasetPath, datasetAttributes, dataBlock);
			}
		}

	}

	public <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> dataBlocks,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		// block until the write is complete
		exec.submit(() ->  {
			writeRegion(datasetPath, datasetAttributes, min, size, dataBlocks, writeFully);
		}).get();
	}

	@Override
	public boolean deleteChunk(String pathName, final long... gridPosition) throws N5Exception {

		if (pathName.equals(""))
			pathName = "/";

		final DatasetAttributes datasetAttributes = getDatasetAttributes(pathName);
		return deleteChunk(pathName, datasetAttributes, gridPosition);
	}

	@Override
	public boolean deleteChunk(String datasetPath, final DatasetAttributes datasetAttributes, final long... gridPosition) throws N5Exception {

		// deletion is not supported in HDF5, so the chunk is overwritten with zeros instead
		// Consider using defaultValue instead of zero?

		if (datasetPath.equals(""))
			datasetPath = "/";

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
				writeChunk(datasetPath, datasetAttributes, empty);
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean deleteBlock(final String pathName, final long... gridPosition) throws N5Exception {

		// HDF5 does not support sharding, so deleteBlock is always equivalent to deleteChunk
		return deleteChunk(pathName, gridPosition);
	}

	@Override
	public boolean deleteBlock(final String datasetPath, final DatasetAttributes datasetAttributes, final long... gridPosition) throws N5Exception {

		// HDF5 does not support sharding, so deleteBlock is always equivalent to deleteChunk
		return deleteChunk(datasetPath, datasetAttributes, gridPosition);
	}

	@Override
	public boolean remove() {

		openDataSetCache.close();
		final File file = writer.file().getFile();
		writer.close();
		return file.delete();
	}

	private static IHDF5Writer openHdf5Writer(final String hdf5Path) {

		final File file = hdf5File(hdf5Path);
		if (file.exists() && !HDF5Utils.isHDF5(file)) {
			throw new N5Exception("File exists at " + file + " and is not a valid HDF5 file");
		}

		try {
			return HDF5Factory.open(file);
		} catch (HDF5Exception e) {
			throw new N5IOException("Cannot open HDF5 Writer", new IOException(e));
		}
	}

}
