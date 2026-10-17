package org.janelia.saalfeldlab.n5.hdf5;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.factory.spi.HDF5Options;
import org.janelia.saalfeldlab.n5.universe.factory.spi.Hdf5Provider;

/**
 * Default {@link Hdf5Provider}, backed by {@code n5-hdf5} and jhdf5.
 * <p>
 * Stateless: everything it needs comes from the {@link HDF5Options} passed in.
 */
public class DefaultHdf5Provider implements Hdf5Provider {

	@Override
	public N5Reader buildReader(final String containerPath, final HDF5Options options) {

		return new N5HDF5Reader(containerPath, options.overrideBlockSize(), options.gsonBuilder(),
				options.defaultBlockSize());
	}

	@Override
	public N5Writer buildWriter(final String containerPath, final HDF5Options options) {

		return new N5HDF5Writer(containerPath, options.overrideBlockSize(), options.gsonBuilder(),
				options.defaultBlockSize());
	}
}
