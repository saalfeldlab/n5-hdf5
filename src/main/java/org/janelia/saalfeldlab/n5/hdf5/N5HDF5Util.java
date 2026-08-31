package org.janelia.saalfeldlab.n5.hdf5;

import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception;

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
import static org.janelia.saalfeldlab.n5.DataType.FLOAT32;
import static org.janelia.saalfeldlab.n5.DataType.FLOAT64;
import static org.janelia.saalfeldlab.n5.DataType.INT16;
import static org.janelia.saalfeldlab.n5.DataType.INT32;
import static org.janelia.saalfeldlab.n5.DataType.INT64;
import static org.janelia.saalfeldlab.n5.DataType.INT8;
import static org.janelia.saalfeldlab.n5.DataType.STRING;
import static org.janelia.saalfeldlab.n5.DataType.UINT16;
import static org.janelia.saalfeldlab.n5.DataType.UINT32;
import static org.janelia.saalfeldlab.n5.DataType.UINT64;
import static org.janelia.saalfeldlab.n5.DataType.UINT8;

final class N5HDF5Util {

	private N5HDF5Util() {}

	/**
	 * Get the HDF5 datatype identifier for the given N5 {@link DataType}.
	 */
	static long toH5T(final DataType dataType) {
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
			throw new IllegalStateException("MemTypeId for STRING is not defined and should not be queried.");
		default:
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Get the N5 {@code DataType} corresponding to the given {@link HDF5DataTypeInformation}.
	 *
	 * @throws N5Exception
	 * 		if the {@code typeInfo} cannot be mapped to a known N5 {@code DataType}
	 */
	static DataType toDataType(final HDF5DataTypeInformation typeInfo) {

		final Class<?> type = typeInfo.tryGetJavaType();
		final boolean signed = typeInfo.isSigned();

		if (type.isAssignableFrom(long.class))
			return signed ? INT64 : UINT64;
		else if (type.isAssignableFrom(int.class))
			return signed ? INT32 : UINT32;
		else if (type.isAssignableFrom(short.class))
			return signed ? INT16 : UINT16;
		else if (type.isAssignableFrom(byte.class))
			return signed ? INT8 : UINT8;
		else if (type.isAssignableFrom(double.class))
			return FLOAT64;
		else if (type.isAssignableFrom(float.class))
			return FLOAT32;
		else if (type.isAssignableFrom(String.class))
			return STRING;

		throw new N5Exception("Datasets of type " + typeInfo + " not yet implemented.");
	}


	/**
	 * Reorder {@code long[]} array representing column-major coordinate (imglib2)
	 * to row-major (hdf5) or vice versa. Permuted in is stored in {@code out}
	 * and {@code out} is returned.
	 *
	 * @param in
	 * 		original coordinates (column/row major)
	 * @param out
	 * 		permuted coordinates (row/column major)
	 *
	 * @return out
	 */
	static long[] reorder(final long[] in, final long[] out) {
		assert in.length == out.length;
		for (int i = 0, o = in.length - 1; i < in.length; ++i, --o)
			out[o] = in[i];
		return out;
	}

	/**
	 * Reorder {@code long[]} array representing column-major coordinate (imglib2)
	 * to row-major (hdf5) or vice versa.
	 *
	 * @param in
	 * 		original coordinates (column/row major)
	 *
	 * @return new array with permuted coordinates (row/column major)
	 */
	static long[] reorder(final long[] in) {
		return in == null ? null : reorder(in, new long[in.length]);
	}

	/**
	 * Reorder {@code int[]} array representing column-major coordinate (imglib2)
	 * to row-major (hdf5) or vice versa. Permuted in is stored in {@code out}
	 * and {@code out} is returned.
	 *
	 * @param in
	 * 		original coordinates (column/row major)
	 * @param out
	 * 		permuted coordinates (row/column major)
	 *
	 * @return out
	 */
	static int[] reorder(final int[] in, final int[] out) {
		assert in.length == out.length;
		for (int i = 0, o = in.length - 1; i < in.length; ++i, --o)
			out[o] = in[i];
		return out;
	}

	/**
	 * Reorder {@code int[]} array representing column-major coordinate (imglib2)
	 * to row-major (hdf5) or vice versa.
	 *
	 * @param in
	 * 		original coordinates (column/row major)
	 *
	 * @return new array with permuted coordinates (row/column major)
	 */
	static int[] reorder(final int[] in) {
		return in == null ? null : reorder(in, new int[in.length]);
	}

	/**
	 * Reorder {@code long[]} array representing column-major coordinate (imglib2) to
	 * row-major (hdf5) or vice versa.
	 *
	 * @param array
	 * 		array to be re-ordered in place
	 */
	static void reorderInPlace(final long[] array) {
		if (array == null)
			return;
		for (int i = 0, j = array.length - 1; i < j; ++i, --j) {
			final long a = array[i];
			array[i] = array[j];
			array[j] = a;
		}
	}

	/**
	 * Reorder {@code int[]} array representing column-major coordinate (imglib2) to
	 * row-major (hdf5) or vice versa.
	 *
	 * @param array
	 * 		array to be re-ordered in place
	 */
	static void reorderInPlace(final int[] array) {
		if (array == null)
			return;
		for (int i = 0, j = array.length - 1; i < j; ++i, --j) {
			final int a = array[i];
			array[i] = array[j];
			array[j] = a;
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

}
