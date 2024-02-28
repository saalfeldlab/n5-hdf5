package org.janelia.saalfeldlab.n5.hdf5;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class HDF5Utils {

	public static byte[] HDF5_SIG = {(byte)137, 72, 68, 70, 13, 10, 26, 10};

	public static boolean isHDF5(String path) {

		final File f = new File(path);
		if (!f.exists() || !f.isFile())
			return false;

		try (final FileInputStream in = new FileInputStream(f)) {
			final byte[] sig = new byte[8];
			in.read(sig);
			return Arrays.equals(sig, HDF5_SIG);
		} catch (final IOException e) {
			return false;
		}
	}
}
