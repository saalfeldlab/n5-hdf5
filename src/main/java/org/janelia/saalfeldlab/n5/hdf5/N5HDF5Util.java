package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.hdf5.IHDF5FileLevelReadOnlyHandler;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.hdf5lib.HDFHelper;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.janelia.saalfeldlab.n5.DataType;

import static hdf.hdf5lib.H5.H5Dclose;
import static hdf.hdf5lib.H5.H5Dopen;
import static hdf.hdf5lib.H5.H5Fclose;
import static hdf.hdf5lib.H5.H5Fopen;
import static hdf.hdf5lib.H5.H5Pclose;
import static hdf.hdf5lib.H5.H5open;
import static hdf.hdf5lib.HDF5Constants.H5F_ACC_RDONLY;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_DOUBLE;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_FLOAT;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT16;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT32;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT64;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_INT8;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_UINT16;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_UINT32;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_UINT64;
import static hdf.hdf5lib.HDF5Constants.H5T_NATIVE_UINT8;
import static hdf.hdf5lib.HDF5Constants.H5T_VLEN;

final class N5HDF5Util {

	private N5HDF5Util() {}

	// TODO: make public and move to N5HDF5Utils?
	static long memTypeId(final DataType dataType) {
		switch (dataType) {
		case INT8:
			return H5T_NATIVE_INT8;
		case UINT8:
			return H5T_NATIVE_UINT8;
		case INT16:
			return H5T_NATIVE_INT16;
		case UINT16:
			return H5T_NATIVE_UINT16;
		case INT32:
			return H5T_NATIVE_INT32;
		case UINT32:
			return H5T_NATIVE_UINT32;
		case INT64:
			return H5T_NATIVE_INT64;
		case UINT64:
			return H5T_NATIVE_UINT64;
		case FLOAT32:
			return H5T_NATIVE_FLOAT;
		case FLOAT64:
			return H5T_NATIVE_DOUBLE;
		case STRING:
			return H5T_VLEN;
		default:
			throw new IllegalArgumentException();
		}
	}

	static long[] reorderToLong(final int[] array) {
		final int n = array.length;
		final long[] reordered = new long[n];
		for (int i = 0; i < n; i++)
			reordered[i] = array[n - i - 1];
		return reordered;
	}

	static long[] reorderMultiplyToLong(final long[] in1, final int[] in2) {
		final int n = in1.length;
		final long[] reordered = new long[n];
		for (int i = 0; i < n; i++)
			reordered[i] = in1[n - i - 1] * in2[n - i - 1];
		return reordered;
	}

	static class OpenDataSetCache {

		private static final int MAX_OPEN_DATASETS = 48;

		private final IHDF5Reader reader;

		private final long fileId;

		final long numericConversionXferPropertyListID;

		private final Map<String, OpenDataSet> cache;

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
			this.reader = reader;

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

			if (pathName.equals(""))
				pathName = "/";

			return reader.exists(pathName) && reader.object().isDataSet(pathName);
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
}
