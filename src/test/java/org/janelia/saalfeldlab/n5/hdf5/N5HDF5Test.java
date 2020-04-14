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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class N5HDF5Test extends AbstractN5Test {

	private static String testDirPath = System.getProperty("user.home") + "/tmp/n5-test.hdf5";
	private static int[] defaultBlockSize = new int[]{5, 6, 7};
	private static IHDF5Writer hdf5Writer;

	public static class Structured {

		public String name = "";
		public int id = 0;
		public double[] data = new double[0];

		@Override
		public boolean equals(Object other) {

			if (other instanceof Structured) {

				Structured otherStructured = (Structured)other;
				return
						name.equals(otherStructured.name) &&
						id == otherStructured.id &&
						Arrays.equals(data, otherStructured.data);
			}
			return false;
		}
	}

	@Before
	public void before() throws IOException {

		after();
		n5 = createN5Writer();
	}

	public void after() throws IOException {

		if (n5 != null) {
			assertTrue(n5.remove());
			n5 = null;
			hdf5Writer = null;
		} else if (hdf5Writer != null) {
			hdf5Writer.close();
			hdf5Writer = null;
		}
	}


	@Override
	protected Compression[] getCompressions() {

		return new Compression[]{
				new RawCompression(),
				new GzipCompression()};
	}

	@Override
	protected N5Writer createN5Writer() throws IOException {

		Files.createDirectories(Paths.get(testDirPath).getParent());
		hdf5Writer = HDF5Factory.open(testDirPath);
		return new N5HDF5Writer(hdf5Writer);
	}

	@Override
	@Test
	@Ignore("HDF5 does not currently support mode 1 data blocks.")
	public void testMode1WriteReadByteBlock() {
	}

	@Override
	protected boolean testDeleteIsBlockDeleted(final DataBlock<?> dataBlock) {

		// deletion is not supported in HDF5, so the block is overwritten with zeroes instead

		if (dataBlock instanceof ByteArrayDataBlock)
			return Arrays.equals((byte[]) dataBlock.getData(), new byte[dataBlock.getNumElements()]);
		else if (dataBlock instanceof ShortArrayDataBlock)
			return Arrays.equals((short[]) dataBlock.getData(), new short[dataBlock.getNumElements()]);
		else if (dataBlock instanceof IntArrayDataBlock)
			return Arrays.equals((int[]) dataBlock.getData(), new int[dataBlock.getNumElements()]);
		else if (dataBlock instanceof LongArrayDataBlock)
			return Arrays.equals((long[]) dataBlock.getData(), new long[dataBlock.getNumElements()]);
		else if (dataBlock instanceof FloatArrayDataBlock)
			return Arrays.equals((float[]) dataBlock.getData(), new float[dataBlock.getNumElements()]);
		else if (dataBlock instanceof DoubleArrayDataBlock)
			return Arrays.equals((double[]) dataBlock.getData(), new double[dataBlock.getNumElements()]);

		return false;
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

		n5.setAttribute("/", N5Reader.VERSION_KEY, N5HDF5Reader.VERSION.toString());
	}

	@Test
	public void testOverrideBlockSize() throws IOException {

		final DatasetAttributes attributes = new DatasetAttributes(dimensions, blockSize, DataType.INT8, new GzipCompression());
		n5.createDataset(datasetName, attributes);

		hdf5Writer.close();
		hdf5Writer = null;
		n5 = null;

		IHDF5Reader hdf5Reader = HDF5Factory.openForReading(testDirPath);
		final N5HDF5Reader n5Reader = new N5HDF5Reader(hdf5Reader, defaultBlockSize);
		final DatasetAttributes originalAttributes = n5Reader.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(blockSize, originalAttributes.getBlockSize());

		final N5HDF5Reader n5ReaderOverride = new N5HDF5Reader(hdf5Reader, true, defaultBlockSize);
		final DatasetAttributes overriddenAttributes = n5ReaderOverride.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(defaultBlockSize, overriddenAttributes.getBlockSize());

		hdf5Reader.close();
	}

	@Test
	public void testDefaultBlockSizeGetter() throws IOException {
		// do not pass array
		{
			try (final N5HDF5Reader h5  = new N5HDF5Writer(testDirPath)) {
				Assert.assertArrayEquals(new int[]{}, h5.getDefaultBlockSizeCopy());
			}
		}
		// pass array
		{
			try (final N5HDF5Reader h5 = new N5HDF5Writer(testDirPath, defaultBlockSize)) {
				Assert.assertArrayEquals(defaultBlockSize, h5.getDefaultBlockSizeCopy());
			}
		}
	}

	@Test
	public void testOverrideBlockSizeGetter() throws IOException {
		// default behavior
		{
			try (final N5HDF5Reader h5 = new N5HDF5Writer(testDirPath)) {
				Assert.assertFalse(h5.doesOverrideBlockSize());
			}
		}
		// overrideBlockSize == false
		{
			try (final N5HDF5Reader h5 = new N5HDF5Reader(testDirPath, false)) {
				Assert.assertFalse(h5.doesOverrideBlockSize());
			}
		}
		// overrideBlockSize == false
		{
			try (final N5HDF5Reader h5 = new N5HDF5Reader(testDirPath, true)) {
				Assert.assertTrue(h5.doesOverrideBlockSize());
			}
		}
	}

	@Test
	public void testFilenameGetter() throws IOException {
		try (final N5HDF5Reader h5  = new N5HDF5Writer(testDirPath)) {
			Assert.assertEquals(new File(testDirPath), h5.getFilename());
		}

	}

	@Test
	public void testStructuredAttributes() throws IOException {

		Structured attribute = new Structured();
		attribute.name = "myName";
		attribute.id = 20;
		attribute.data = new double[] {1, 2, 3, 4};

		n5.createGroup("/structuredAttributes");
		n5.setAttribute("/structuredAttributes", "myAttribute", attribute);
		Structured readAttribute = n5.getAttribute("/structuredAttributes", "myAttribute", Structured.class);
		assertEquals(attribute, readAttribute);

		n5.remove("/structuredAttributes");
	}
}
