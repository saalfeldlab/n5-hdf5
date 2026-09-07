package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.hdf5.IHDF5FileLevelReadOnlyHandler;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.hdf5lib.HDFHelper;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;
import hdf.hdf5lib.structs.H5O_info_t;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static hdf.hdf5lib.H5.H5Dclose;
import static hdf.hdf5lib.H5.H5Dopen;
import static hdf.hdf5lib.H5.H5Fclose;
import static hdf.hdf5lib.H5.H5Fopen;
import static hdf.hdf5lib.H5.H5Pclose;
import static hdf.hdf5lib.H5.H5open;
import static hdf.hdf5lib.HDF5Constants.H5F_ACC_RDONLY;
import static hdf.hdf5lib.HDF5Constants.H5O_TYPE_DATASET;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;

/**
 * An LRU cache of open HDF5 dataset ids.
 * <p>
 * Reading a chunk through the high-level JHDF5 API opens the dataset, reads
 * the chunk, and closes the dataset again. Typical N5 usage reads many
 * chunks from the same few datasets, and that constant opening and closing
 * dominates the time spent.
 * <p>
 * Cached datasets are reference-counted. {@link #get} returns a retained
 * {@link OpenDataSet} which the caller has to {@link OpenDataSet#close
 * close} again (it is {@code AutoCloseable}, so use try-with-resources), or
 * {@code null} if the path is not a dataset. Evicting or {@link #remove
 * removing} an entry only drops the cache's own reference: the id stays
 * valid until its last user closes it.
 * <p>
 * The cache owns an HDF5 file handle and a data transfer property list, and
 * therefore has to be {@link #close closed} ({@code N5HDF5Reader.close()}
 * does that). {@link #remove} has to be called whenever a dataset is deleted
 * or re-created, otherwise chunk IO can go through a stale dataset id.
 */
class OpenDataSetCache {

	private static final int MAX_OPEN_DATASETS = 48;

	private final long fileId;

	final long numericConversionXferPropertyListID;

	private final Map<String, OpenDataSet> cache;

	/**
	 * A reference-counted open HDF5 dataset id. {@link #close} releases one
	 * reference; the dataset is closed when the last one is released.
	 */
	class OpenDataSet implements AutoCloseable {

		private final AtomicInteger refcount;

		final long dataSetId;

		public OpenDataSet(final String pathName) {
			refcount = new AtomicInteger(1);
			dataSetId = H5Dopen(fileId, pathName, H5P_DEFAULT);
		}

		public void retain() {
			if (refcount.getAndIncrement() <= 0)
				throw new IllegalStateException();
		}

		@Override
		public void close() {
			if (refcount.decrementAndGet() == 0)
				H5Dclose(dataSetId);
		}
	}

	public OpenDataSetCache(final IHDF5Reader reader) {

		// TODO: Do see ch.systemsx.cisd.hdf5.HDF5.createFileAccessPropertyListId for version bounds checking
		final long fileAccessPropertyListId = H5P_DEFAULT;

		final IHDF5FileLevelReadOnlyHandler fileHandler = reader.file();
		final boolean performNumericConversions = fileHandler.isPerformNumericConversions();
		final File file = fileHandler.getFile();

		// Make sure library is initialized. This can be called multiple times.
		H5open();

		// See ch.systemsx.cisd.hdf5.HDF5 constructor
		// Make sure to close the numericConversionXferPropertyListID property list created below. See close()
		if (performNumericConversions) {
			numericConversionXferPropertyListID = HDFHelper.H5Pcreate_xfer_abort_overflow();
		} else {
			numericConversionXferPropertyListID = HDFHelper.H5Pcreate_xfer_abort();
		}

		// Make sure to close the fileID created below. See close()
		fileId = H5Fopen(file.getAbsolutePath(), H5F_ACC_RDONLY, fileAccessPropertyListId);

		cache = new LinkedHashMap<String, OpenDataSet>(MAX_OPEN_DATASETS, 0.75f, true) {

			@Override
			protected boolean removeEldestEntry(final Map.Entry<String, OpenDataSet> eldest) {
				if (size() > MAX_OPEN_DATASETS) {
					final OpenDataSet dataSet = eldest.getValue();
					if (dataSet != null)
						dataSet.close();
					return true;
				} else {
					return false;
				}
			}
		};
	}

	private boolean datasetExists(String pathName) {

		if ("".equals(pathName) || "/".equals(pathName))
			return false;

		try {
			final H5O_info_t info = H5.H5Oget_info_by_name(fileId, pathName, HDF5Constants.H5O_INFO_BASIC, HDF5Constants.H5P_DEFAULT);
			return info.type == H5O_TYPE_DATASET;
		} catch (HDF5LibraryException e) {
			return false;
		}
	}

	public synchronized OpenDataSet get(final String pathName) {
		OpenDataSet dataSet = cache.get(pathName);
		if (dataSet == null && datasetExists(pathName)) {
			dataSet = new OpenDataSet(pathName);
			cache.put(pathName, dataSet);
		}
		if (dataSet != null)
			dataSet.retain();
		return dataSet;
	}

	public synchronized void remove(final String pathName) {
		final OpenDataSet dataSet = cache.remove(pathName);
		if (dataSet != null)
			dataSet.close();
	}

	// close and remove all datasets in the cache
	public synchronized void clear() {
		cache.values().forEach(OpenDataSet::close);
		cache.clear();
	}

	private boolean isClosed = false;

	public synchronized void close() {
		clear();
		if (!isClosed) {
			isClosed = true;

			int status = H5Pclose(numericConversionXferPropertyListID);
			if (status < 0) {
				throw new RuntimeException("Error closing property list");
			}
			status = H5Fclose(fileId);
			if (status < 0) {
				throw new RuntimeException("Error closing file");
			}
		}
	}
}
