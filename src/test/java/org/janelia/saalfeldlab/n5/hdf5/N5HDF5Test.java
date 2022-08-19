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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 * @author John Bogovic
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
		public boolean equals(final Object other) {

			if (other instanceof Structured) {

				final Structured otherStructured = (Structured)other;
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
	public void testCreateGroup() {

		try {
			n5.createGroup(groupName);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final Path groupPath = Paths.get(groupName);
		for (int i = 0; i < groupPath.getNameCount(); ++i)
			//replace `\` with `/` in the case of Windows
			if (!n5.exists(groupPath.subpath(0, i + 1)
					.toString()
					.replace(File.separatorChar, '/')))
				fail("Group does not exist");
	}

	@Override
	@Test
	@Ignore("HDF5 does not currently support mode 1 data blocks.")
	public void testMode1WriteReadByteBlock() {
	}

	@Override
	@Test
	@Ignore("HDF5 does not currently support mode 2 data blocks and serialized objects.")
	public void testWriteReadSerializableBlock() {
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

		final IHDF5Reader hdf5Reader = HDF5Factory.openForReading(testDirPath);
		final N5HDF5Reader n5Reader = new N5HDF5Reader(hdf5Reader, defaultBlockSize);
		final DatasetAttributes originalAttributes = n5Reader.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(blockSize, originalAttributes.getBlockSize());

		final N5HDF5Reader n5ReaderOverride = new N5HDF5Reader(hdf5Reader, true, defaultBlockSize);
		final DatasetAttributes overriddenAttributes = n5ReaderOverride.getDatasetAttributes(datasetName);
		Assert.assertArrayEquals(defaultBlockSize, overriddenAttributes.getBlockSize());

		n5Reader.close();
		n5ReaderOverride.close();
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

		final Structured attribute = new Structured();
		attribute.name = "myName";
		attribute.id = 20;
		attribute.data = new double[] {1, 2, 3, 4};

		n5.createGroup("/structuredAttributes");
		n5.setAttribute("/structuredAttributes", "myAttribute", attribute);

		/* class interface */
		Structured readAttribute = n5.getAttribute("/structuredAttributes", "myAttribute", Structured.class);
		assertEquals(attribute, readAttribute);

		/* type interface */
		readAttribute = n5.getAttribute("/structuredAttributes", "myAttribute", new TypeToken<Structured>(){}.getType());
		assertEquals(attribute, readAttribute);

		n5.remove("/structuredAttributes");
	}

	@Test
	public void testAttributesAsJson() throws IOException {

		N5HDF5Writer h5 = (N5HDF5Writer) n5;
		final Structured attribute = new Structured();
		attribute.name = "myName";
		attribute.id = 20;
		attribute.data = new double[] {1, 2, 3, 4};

		final String string = "a string";
		final String emptyString = "";
		final double[] darray = new double[] {0.1,0.2,0.3,0.4,0.5};
		final int[] iarray = new int[] {1,2,3,4,5};

		final JsonElement oElem = h5.getGson().toJsonTree( attribute );
		final JsonElement sElem = h5.getGson().toJsonTree( string );
		final JsonElement esElem = h5.getGson().toJsonTree( emptyString );
		final JsonElement dElem = h5.getGson().toJsonTree( darray );
		final JsonElement iElem = h5.getGson().toJsonTree( iarray );

		n5.createGroup("/attributeTest");
		n5.setAttribute("/attributeTest", "myAttribute", attribute);
		n5.setAttribute("/attributeTest", "string", string );
		n5.setAttribute("/attributeTest", "emptyString", emptyString );
		n5.setAttribute("/attributeTest", "darray", darray );
		n5.setAttribute("/attributeTest", "iarray", iarray );

		HashMap<String, JsonElement> attrs = h5.getAttributes( "/attributeTest" );
		assertTrue( "has object attribute", attrs.containsKey("myAttribute") );
		assertTrue( "has string attribute", attrs.containsKey("string") );
		assertTrue( "has empty string attribute", attrs.containsKey("emptyString") );
		assertTrue( "has d-array attribute", attrs.containsKey("darray") );
		assertTrue( "has i-array attribute", attrs.containsKey("iarray") );

		assertEquals("object elem", oElem, attrs.get("myAttribute"));
		assertEquals("string elem", sElem, attrs.get("string"));
		assertEquals("empty string elem", esElem, attrs.get("emptyString"));
		assertEquals("double array elem", dElem, attrs.get("darray"));
		assertEquals("int array elem", iElem, attrs.get("iarray"));

		n5.remove("/attributeTest");

		// ensure dataset attributes are mapped
		long[] dims = new long[] {4,4};
		int[] blkSz = new int[] {4,4};
		RawCompression compression = new RawCompression();

		n5.createDataset( "/datasetTest", dims, blkSz, DataType.UINT8, compression );
		HashMap<String, JsonElement> dsetAttrs = h5.getAttributes( "/datasetTest" );
		assertTrue( "dset has dimensions", dsetAttrs.containsKey( "dimensions" ));
		assertTrue( "dset has blockSize", dsetAttrs.containsKey( "blockSize" ));
		assertTrue( "dset has dataType", dsetAttrs.containsKey( "dataType" ));
		assertTrue( "dset has compression", dsetAttrs.containsKey( "compression" ));

		final Gson gson = h5.getGson();
		assertArrayEquals( "dset dimensions", dims, gson.fromJson( dsetAttrs.get( "dimensions" ), long[].class ));
		assertArrayEquals( "dset blockSize", blkSz, gson.fromJson( dsetAttrs.get( "blockSize" ), int[].class ));
		assertEquals( "dset dataType", DataType.UINT8, gson.fromJson( dsetAttrs.get( "dataType" ), DataType.class ));

		n5.remove("/datasetTest");
	}

	@Test
	public void testType() {
		System.out.println(new TypeToken<DataType>() {}.getType().getTypeName());
	}
}
