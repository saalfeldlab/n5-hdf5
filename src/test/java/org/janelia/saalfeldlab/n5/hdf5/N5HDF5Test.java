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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

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

	private static String testFilePath;
	private static int[] defaultBlockSize = new int[]{5, 6, 7};
	private static IHDF5Writer hdf5Writer;

	static {
		try {
			testFilePath = Files.createTempFile( "n5-hdf5-test-", ".hdf5" ).toFile().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

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

		Files.createDirectories(Paths.get( testFilePath ).getParent());
		hdf5Writer = HDF5Factory.open( testFilePath );
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

		final IHDF5Reader hdf5Reader = HDF5Factory.openForReading( testFilePath );
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
			try (final N5HDF5Reader h5  = new N5HDF5Writer( testFilePath )) {
				Assert.assertArrayEquals(new int[]{}, h5.getDefaultBlockSizeCopy());
			}
		}
		// pass array
		{
			try (final N5HDF5Reader h5 = new N5HDF5Writer( testFilePath, defaultBlockSize)) {
				Assert.assertArrayEquals(defaultBlockSize, h5.getDefaultBlockSizeCopy());
			}
		}
	}

	@Test
	public void testOverrideBlockSizeGetter() throws IOException {
		// default behavior
		{
			try (final N5HDF5Reader h5 = new N5HDF5Writer( testFilePath )) {
				Assert.assertFalse(h5.doesOverrideBlockSize());
			}
		}
		// overrideBlockSize == false
		{
			try (final N5HDF5Reader h5 = new N5HDF5Reader( testFilePath, false)) {
				Assert.assertFalse(h5.doesOverrideBlockSize());
			}
		}
		// overrideBlockSize == false
		{
			try (final N5HDF5Reader h5 = new N5HDF5Reader( testFilePath, true)) {
				Assert.assertTrue(h5.doesOverrideBlockSize());
			}
		}
	}

	@Test
	public void testFilenameGetter() throws IOException {
		try (final N5HDF5Reader h5  = new N5HDF5Writer( testFilePath )) {
			Assert.assertEquals(new File( testFilePath ), h5.getFilename());
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
	
	@Override
	@Test
	/*
	 * Differs from AbstractN5Test since an int will be read back as int, not a long
	 */
	public void testListAttributes() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, new RawCompression());
			n5.setAttribute(datasetName2, "attr1", new double[] {1.1, 2.1, 3.1});
			n5.setAttribute(datasetName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(datasetName2, "attr3", 1.1);
			n5.setAttribute(datasetName2, "attr4", "a");
			n5.setAttribute(datasetName2, "attr5", new long[] {1, 2, 3});
			n5.setAttribute(datasetName2, "attr6", 1);
			n5.setAttribute(datasetName2, "attr7", new double[] {1, 2, 3.1});
			n5.setAttribute(datasetName2, "attr8", new Object[] {"1", 2, 3.1});

			Map<String, Class<?>> attributesMap = n5.listAttributes(datasetName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);
			Assert.assertTrue(attributesMap.get("attr5") == long[].class);
			//HDF5 will parse an int as an int rather than a long
			Assert.assertTrue(attributesMap.get("attr6") == int.class);
			Assert.assertTrue(attributesMap.get("attr7") == double[].class);
			Assert.assertTrue(attributesMap.get("attr8") == Object[].class);

			n5.createGroup(groupName2);
			n5.setAttribute(groupName2, "attr1", new double[] {1.1, 2.1, 3.1});
			n5.setAttribute(groupName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(groupName2, "attr3", 1.1);
			n5.setAttribute(groupName2, "attr4", "a");
			n5.setAttribute(groupName2, "attr5", new long[] {1, 2, 3});
			n5.setAttribute(groupName2, "attr6", 1);
			n5.setAttribute(groupName2, "attr7", new double[] {1, 2, 3.1});
			n5.setAttribute(groupName2, "attr8", new Object[] {"1", 2, 3.1});

			attributesMap = n5.listAttributes(groupName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);
			Assert.assertTrue(attributesMap.get("attr5") == long[].class);
			//HDF5 will parse an int as an int rather than a long
			Assert.assertTrue(attributesMap.get("attr6") == int.class);
			Assert.assertTrue(attributesMap.get("attr7") == double[].class);
			Assert.assertTrue(attributesMap.get("attr8") == Object[].class);
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}
	
	@Override
	@Test
	public void testDeepList() {
		try {

			// clear container to start
			n5.remove();
			// create a new container since the above removes the file
			n5 = createN5Writer();

			n5.createGroup(groupName);
			for (final String subGroup : subGroupNames)
				n5.createGroup(groupName + "/" + subGroup);

			final List<String> groupsList = Arrays.asList(n5.deepList("/"));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", groupsList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", Arrays.asList(n5.deepList("")).contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			final DatasetAttributes datasetAttributes = new DatasetAttributes(dimensions, blockSize, DataType.UINT64, new RawCompression());
			final LongArrayDataBlock dataBlock = new LongArrayDataBlock( blockSize, new long[]{0,0,0}, new long[blockNumElements] );
			n5.createDataset(datasetName, datasetAttributes );
			n5.writeBlock(datasetName, datasetAttributes, dataBlock);

			final List<String> datasetList = Arrays.asList(n5.deepList("/"));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertFalse("deepList stops at datasets", datasetList.contains(datasetName + "/0"));

			final List<String> datasetList2 = Arrays.asList(n5.deepList(""));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetList2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetList2.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetList2.contains(datasetName + "/0"));

			final String prefix = "/test";
			final String datasetSuffix = "group/dataset";
			final List<String> datasetList3 = Arrays.asList(n5.deepList(prefix));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetList3.contains("group/" + subGroup));
			Assert.assertTrue("deepList contents", datasetList3.contains(datasetName.replaceFirst(prefix + "/", "")));

			// parallel deepList tests
			final List<String> datasetListP = Arrays.asList(n5.deepList("/", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetListP.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP.contains(datasetName + "/0"));

			final List<String> datasetListP2 = Arrays.asList(n5.deepList("", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetListP2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP2.contains(datasetName.replaceFirst("/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP2.contains(datasetName + "/0"));

			final List<String> datasetListP3 = Arrays.asList(n5.deepList(prefix, Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				Assert.assertTrue("deepList contents", datasetListP3.contains("group/" + subGroup));
			Assert.assertTrue("deepList contents", datasetListP3.contains(datasetName.replaceFirst(prefix + "/", "")));
			Assert.assertFalse("deepList stops at datasets", datasetListP3.contains(datasetName + "/0"));

			// test filtering
			final Predicate<String> isCalledDataset = d -> {
				return d.endsWith("/dataset");
			};
			final Predicate<String> isBorC = d -> {
				return d.matches(".*/[bc]$");
			};

			final List<String> datasetListFilter1 = Arrays.asList(n5.deepList(prefix, isCalledDataset));
			Assert.assertTrue(
					"deepList filter \"dataset\"",
					datasetListFilter1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilter2 = Arrays.asList(n5.deepList(prefix, isBorC));
			Assert.assertTrue(
					"deepList filter \"b or c\"",
					datasetListFilter2.stream().map(x -> prefix + x).allMatch(isBorC));

			final List<String> datasetListFilterP1 =
					Arrays.asList(n5.deepList(prefix, isCalledDataset, Executors.newFixedThreadPool(2)));
			Assert.assertTrue(
					"deepList filter \"dataset\"",
					datasetListFilterP1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilterP2 =
					Arrays.asList(n5.deepList(prefix, isBorC, Executors.newFixedThreadPool(2)));
			Assert.assertTrue(
					"deepList filter \"b or c\"",
					datasetListFilterP2.stream().map(x -> prefix + x).allMatch(isBorC));

			// test dataset filtering
			final List<String> datasetListFilterD = Arrays.asList(n5.deepListDatasets(prefix));
			Assert.assertTrue(
					"deepListDataset",
					datasetListFilterD.size() == 1 && (prefix + "/" + datasetListFilterD.get(0)).equals(datasetName));
			Assert.assertArrayEquals(
					datasetListFilterD.toArray(),
					n5.deepList(
							prefix,
							a -> {
								try { return n5.datasetExists(a); }
								catch (final IOException e) { return false; }
							}));

			final List<String> datasetListFilterDandBC = Arrays.asList(n5.deepListDatasets(prefix, isBorC));
			Assert.assertTrue("deepListDatasetFilter", datasetListFilterDandBC.size() == 0);
			Assert.assertArrayEquals(
					datasetListFilterDandBC.toArray(),
					n5.deepList(
							prefix,
							a -> {
								try { return n5.datasetExists(a) && isBorC.test(a); }
								catch (final IOException e) { return false; }
							}));

			final List<String> datasetListFilterDP =
					Arrays.asList(n5.deepListDatasets(prefix, Executors.newFixedThreadPool(2)));
			Assert.assertTrue(
					"deepListDataset Parallel",
					datasetListFilterDP.size() == 1 && (prefix + "/" + datasetListFilterDP.get(0)).equals(datasetName));
			Assert.assertArrayEquals(
					datasetListFilterDP.toArray(),
					n5.deepList(
							prefix,
							a -> {
								try { return n5.datasetExists(a); }
								catch (final IOException e) { return false; }
							},
							Executors.newFixedThreadPool(2)));

			final List<String> datasetListFilterDandBCP =
					Arrays.asList(n5.deepListDatasets(prefix, isBorC, Executors.newFixedThreadPool(2)));
			Assert.assertTrue("deepListDatasetFilter Parallel", datasetListFilterDandBCP.size() == 0);
			Assert.assertArrayEquals(
					datasetListFilterDandBCP.toArray(),
					n5.deepList(
							prefix,
							a -> {
								try { return n5.datasetExists(a) && isBorC.test(a); }
								catch (final IOException e) { return false; }
							},
							Executors.newFixedThreadPool(2)));

		} catch (final IOException | InterruptedException | ExecutionException e) {
			fail(e.getMessage());
		}
	}
}
