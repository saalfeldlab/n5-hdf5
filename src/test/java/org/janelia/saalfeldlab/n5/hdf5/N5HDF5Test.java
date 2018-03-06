/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.hdf5;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public class N5HDF5Test extends AbstractN5Test {

	private static String testDirPath = System.getProperty("user.home") + "/tmp/n5-test.hdf5";
	private static int[] defaultBlockSize = new int[]{5, 6, 7};

	@Override
	protected Compression[] getCompressions() {

		return new Compression[]{
				new RawCompression(),
				new GzipCompression()};
	}

	@Override
	protected N5Writer createN5Writer() throws IOException {

		return new N5HDF5Writer(testDirPath);

	}

	@Override
	@Test
	@Ignore("HDF5 does not currently support mode 1 data blocks.")
	public void testMode1WriteReadByteBlock() {
	}

	@Override
	@Test
	public void testVersion() throws NumberFormatException, IOException {

		final Version n5Version = n5.getVersion();

		System.out.println(n5Version);

		Assert.assertTrue(n5Version.equals(N5HDF5Reader.VERSION));

		n5.setAttribute("/", N5Reader.VERSION_KEY,
				new Version(N5HDF5Reader.VERSION.getMajor() + 1, N5HDF5Reader.VERSION.getMinor(), N5HDF5Reader.VERSION.getPatch()).toString());

		Assert.assertFalse(N5HDF5Reader.VERSION.isCompatible(n5.getVersion()));
	}

	public void testOverrideBlockSize() throws IOException {

		final N5Writer n5Writer = createN5Writer();
		final DatasetAttributes attributes = new DatasetAttributes(dimensions, blockSize, DataType.INT8, new GzipCompression());
		n5Writer.createDataset(datasetName, attributes);

		final N5HDF5Reader n5Reader = new N5HDF5Reader(testDirPath, defaultBlockSize);
		final DatasetAttributes originalAttributes = n5Reader.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(blockSize, originalAttributes.getBlockSize());

		final N5HDF5Reader n5ReaderOverride = new N5HDF5Reader(testDirPath, true, defaultBlockSize);
		final DatasetAttributes overriddenAttributes = n5ReaderOverride.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(defaultBlockSize, originalAttributes.getBlockSize());

	}

}
