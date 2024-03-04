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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import hdf.hdf5lib.exceptions.HDF5SymbolTableException;
import org.apache.commons.io.FileUtils;
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
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 * @author John Bogovic
 */
public class N5HDF5Test extends AbstractN5Test {

	private static final Random random = new Random();
	private static final Path TEMP_DIR;

	static {
		try {
			TEMP_DIR = Files.createTempDirectory("hdf5-tests-");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@AfterClass
	public static void deleteTempDirectory() throws IOException {

		final File tempDir = TEMP_DIR.toFile();
		if (tempDir.exists()) {
			FileUtils.deleteDirectory(tempDir);
		}
	}

	private static final int[] defaultBlockSize = new int[]{5, 6, 7};
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

	@Override
	protected Compression[] getCompressions() {

		return new Compression[]{
				new RawCompression(),
				new GzipCompression()};
	}

	@Override
	protected String tempN5Location() throws IOException {

		String tmpFile = "n5-hdf5-test-" + Long.toUnsignedString(random.nextLong()) + ".hdf5";
		return TEMP_DIR.resolve(tmpFile).toFile().getCanonicalPath();

	}

	@Override protected N5HDF5Writer createN5Writer() throws IOException {

		final String location = tempN5Location();
		final String hdf5Path = resolveTestHdf5Path(location);
		Files.deleteIfExists(Paths.get(location));
		Files.deleteIfExists(Paths.get(hdf5Path));
		return new N5HDF5Writer(hdf5Path, false, new GsonBuilder());
	}

	@Override protected N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException {

		return new N5HDF5Writer(resolveTestHdf5Path(location), false, gson);
	}

	@Override protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

		return new N5HDF5Reader(resolveTestHdf5Path(location), false, gson);
	}

	@Override
	protected N5Writer createN5Writer(String location) throws IOException {

		return new N5HDF5Writer(resolveTestHdf5Path(location));
	}

	private static String resolveTestHdf5Path(String location) throws IOException {

		Path locationPath;
		try {
			final URI locationUri = N5HDF5Reader.FILE_SYSTEM_KEY_VALUE_ACCESS.uri(location);
			locationPath = FileSystems.getDefault().provider().getPath(locationUri);
		} catch (URISyntaxException e) {
			locationPath = Paths.get(location);
		}
		if (Files.isDirectory(locationPath)) {
			locationPath = locationPath.resolve("test.hdf5");
		}
		return locationPath.toFile().getCanonicalPath();
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
	public void testVersion() throws NumberFormatException {

		try (N5Writer n5 = createTempN5Writer()) {

			final Version n5Version = n5.getVersion();

			assertEquals(n5Version, N5HDF5Reader.VERSION);

			n5.setAttribute("/", N5Reader.VERSION_KEY,
					new Version(N5HDF5Reader.VERSION.getMajor() + 1, N5HDF5Reader.VERSION.getMinor(), N5HDF5Reader.VERSION.getPatch()).toString());

			assertFalse(N5HDF5Reader.VERSION.isCompatible(n5.getVersion()));

			n5.setAttribute("/", N5Reader.VERSION_KEY, N5HDF5Reader.VERSION.toString());
		}
	}

	@Override
	@Test
	public void testSetAttributeDoesntCreateGroup() {

		try (final N5Writer writer = createTempN5Writer()) {
			final String testGroup = "/group/should/not/exit";
			assertFalse(writer.exists(testGroup));
			assertThrows(HDF5SymbolTableException.class, () -> writer.setAttribute(testGroup, "test", "test"));
			assertFalse(writer.exists(testGroup));
		}
	}

	@Test
	public void testOverrideBlockSize() {

		try (N5Writer n5HDF5Writer = createTempN5Writer()) {
			final String testFilePath = n5HDF5Writer.getURI().getPath();
			final DatasetAttributes attributes = new DatasetAttributes(dimensions, blockSize, DataType.INT8, new GzipCompression());
			n5HDF5Writer.createDataset(datasetName, attributes);

			final IHDF5Reader hdf5Reader = HDF5Factory.openForReading(testFilePath);
			try (N5HDF5Reader n5Reader = new N5HDF5Reader(hdf5Reader, defaultBlockSize)) {
				final DatasetAttributes originalAttributes = n5Reader.getDatasetAttributes(datasetName);
				assertArrayEquals(blockSize, originalAttributes.getBlockSize());
				try (N5HDF5Reader n5ReaderOverride = new N5HDF5Reader(hdf5Reader, true, defaultBlockSize)) {
					final DatasetAttributes overriddenAttributes = n5ReaderOverride.getDatasetAttributes(datasetName);
					assertArrayEquals(defaultBlockSize, overriddenAttributes.getBlockSize());
				}
			}

		}
	}

	@Test
	public void testDefaultBlockSizeGetter() throws IOException {
		// do not pass array
		{
			try (final N5HDF5Writer h5 = (N5HDF5Writer)createTempN5Writer()) {
				assertArrayEquals(new int[]{}, h5.getDefaultBlockSizeCopy());
			}
		}
		// pass array
		{
			try (final N5HDF5Writer h5 = new N5HDF5Writer(tempN5Location(), defaultBlockSize)) {
				assertArrayEquals(defaultBlockSize, h5.getDefaultBlockSizeCopy());
				h5.remove();
			}
		}
	}

	@Test
	public void testOverrideBlockSizeGetter() {
		// default behavior
		try (final N5HDF5Writer h5 = (N5HDF5Writer)createTempN5Writer()) {
			final String testFilePath = h5.getURI().getPath();
			assertFalse(h5.doesOverrideBlockSize());
			// overrideBlockSize == false
			try (final N5HDF5Reader asReader = new N5HDF5Reader( testFilePath, false)) {
				assertFalse(asReader.doesOverrideBlockSize());
			}
			// overrideBlockSize == false
			try (final N5HDF5Reader asReader = new N5HDF5Reader( testFilePath, true)) {
				assertTrue(asReader.doesOverrideBlockSize());
			}
		}
	}

	@Test
	public void testFilenameGetter() {

		try (final N5HDF5Writer h5 = (N5HDF5Writer)createTempN5Writer()) {
			final String testFilePath = h5.getURI().getPath();
			assertEquals(new File(testFilePath), h5.getFilename());
		}

	}

	@Test
	public void testStructuredAttributes() {

		try (N5Writer n5 = createTempN5Writer()) {
			final Structured attribute = new Structured();
			attribute.name = "myName";
			attribute.id = 20;
			attribute.data = new double[]{1, 2, 3, 4};

			n5.createGroup("/structuredAttributes");
			n5.setAttribute("/structuredAttributes", "myAttribute", attribute);

			/* class interface */
			Structured readAttribute = n5.getAttribute("/structuredAttributes", "myAttribute", Structured.class);
			assertEquals(attribute, readAttribute);

			/* type interface */
			readAttribute = n5.getAttribute("/structuredAttributes", "myAttribute", new TypeToken<Structured>() {

			}.getType());
			assertEquals(attribute, readAttribute);

			n5.remove("/structuredAttributes");
		}
	}

	@Test
	public void testAttributesAsJson() throws IOException {

		File tmpFile = Files.createTempDirectory("nulls-test-").toFile();
		tmpFile.deleteOnExit();
		String canonicalPath = tmpFile.getCanonicalPath();
		/* serializeNulls*/
		try (N5Writer writer = createN5Writer(canonicalPath, new GsonBuilder().serializeNulls())) {

			N5HDF5Writer h5 = (N5HDF5Writer)writer;
			final Structured attribute = new Structured();
			attribute.name = "myName";
			attribute.id = 20;
			attribute.data = new double[]{1, 2, 3, 4};

			final String string = "a string";
			final String emptyString = "";
			final double[] darray = new double[]{0.1, 0.2, 0.3, 0.4, 0.5};
			final int[] iarray = new int[]{1, 2, 3, 4, 5};

			final JsonElement oElem = h5.getGson().toJsonTree(attribute);
			final JsonElement sElem = h5.getGson().toJsonTree(string);
			final JsonElement esElem = h5.getGson().toJsonTree(emptyString);
			final JsonElement dElem = h5.getGson().toJsonTree(darray);
			final JsonElement iElem = h5.getGson().toJsonTree(iarray);

			writer.createGroup("/attributeTest");
			writer.setAttribute("/attributeTest", "myAttribute", attribute);
			writer.setAttribute("/attributeTest", "string", string);
			writer.setAttribute("/attributeTest", "emptyString", emptyString);
			writer.setAttribute("/attributeTest", "darray", darray);
			writer.setAttribute("/attributeTest", "iarray", iarray);

			JsonElement attrs = h5.getAttributes("/attributeTest");
			assertTrue(attrs.isJsonObject());
			final JsonObject attrsObj = attrs.getAsJsonObject();
			assertTrue("has object attribute", attrsObj.has("myAttribute"));
			assertTrue("has string attribute", attrsObj.has("string"));
			assertTrue("has empty string attribute", attrsObj.has("emptyString"));
			assertTrue("has d-array attribute", attrsObj.has("darray"));
			assertTrue("has i-array attribute", attrsObj.has("iarray"));

			assertEquals("object elem", oElem, attrsObj.get("myAttribute"));
			assertEquals("string elem", sElem, attrsObj.get("string"));
			assertEquals("empty string elem", esElem, attrsObj.get("emptyString"));
			assertEquals("double array elem", dElem, attrsObj.get("darray"));
			assertEquals("int array elem", iElem, attrsObj.get("iarray"));

			writer.remove("/attributeTest");

			// ensure dataset attributes are mapped
			long[] dims = new long[]{4, 4};
			int[] blkSz = new int[]{4, 4};
			RawCompression compression = new RawCompression();

			writer.createDataset("/datasetTest", dims, blkSz, DataType.UINT8, compression);
			JsonElement dsetAttrs = h5.getAttributes("/datasetTest");
			assertTrue(dsetAttrs.isJsonObject());
			final JsonObject dsetAttrsObj = dsetAttrs.getAsJsonObject();
			assertTrue("dset has dimensions", dsetAttrsObj.has("dimensions"));
			assertTrue("dset has blockSize", dsetAttrsObj.has("blockSize"));
			assertTrue("dset has dataType", dsetAttrsObj.has("dataType"));
			assertTrue("dset has compression", dsetAttrsObj.has("compression"));

			final Gson gson = h5.getGson();
			assertArrayEquals("dset dimensions", dims, gson.fromJson(dsetAttrsObj.get("dimensions"), long[].class));
			assertArrayEquals("dset blockSize", blkSz, gson.fromJson(dsetAttrsObj.get("blockSize"), int[].class));
			assertEquals("dset dataType", DataType.UINT8, gson.fromJson(dsetAttrsObj.get("dataType"), DataType.class));

			writer.remove("/datasetTest");
			writer.remove();
		}
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

		try (N5Writer n5 = createTempN5Writer()) {

			final String groupName2 = groupName + "-2";
			final String datasetName2 = datasetName + "-2";
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
			assertSame(attributesMap.get("attr1"), double[].class);
			assertSame(attributesMap.get("attr2"), String[].class);
			assertSame(attributesMap.get("attr3"), double.class);
			assertSame(attributesMap.get("attr4"), String.class);
			assertSame(attributesMap.get("attr5"), long[].class);
			//HDF5 will parse an int as an int rather than a long
			assertSame(attributesMap.get("attr6"), int.class);
			assertSame(attributesMap.get("attr7"), double[].class);
			assertSame(attributesMap.get("attr8"), Object[].class);

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
			assertSame(attributesMap.get("attr1"), double[].class);
			assertSame(attributesMap.get("attr2"), String[].class);
			assertSame(attributesMap.get("attr3"), double.class);
			assertSame(attributesMap.get("attr4"), String.class);
			assertSame(attributesMap.get("attr5"), long[].class);
			//HDF5 will parse an int as an int rather than a long
			assertSame(attributesMap.get("attr6"), int.class);
			assertSame(attributesMap.get("attr7"), double[].class);
			assertSame(attributesMap.get("attr8"), Object[].class);
		}
	}

	@Test
	public void testMatrixAttributes()
	{
		final float[][] fmtx = new float[][]{{0,1},{2,3}};
		final double[][] dmtx = new double[][]{{0,1},{2,3}};
		final byte[][] bmtx = new byte[][]{{0,1},{2,3}};
		final int[][] imtx = new int[][]{{0,1},{2,3}};
		final long[][] lmtx = new long[][]{{0,1},{2,3}};

		try (N5Writer n5 = createTempN5Writer()) {
			n5.createGroup( "mtx" );
			n5.setAttribute( "mtx", "fmtx", fmtx );
			n5.setAttribute( "mtx", "dmtx", dmtx );
			n5.setAttribute( "mtx", "bmtx", bmtx );
			n5.setAttribute( "mtx", "imtx", imtx );
			n5.setAttribute( "mtx", "lmtx", lmtx );

			final float[][] f = n5.getAttribute( "mtx", "fmtx", float[][].class );
			final double[][] d = n5.getAttribute( "mtx", "dmtx", double[][].class );
			final byte[][] b = n5.getAttribute( "mtx", "bmtx", byte[][].class );
			final int[][] i = n5.getAttribute( "mtx", "imtx", int[][].class );
			final long[][] l = n5.getAttribute( "mtx", "lmtx", long[][].class );

			assertArrayEquals( fmtx[ 0 ], f[ 0 ], 1e-9f );
			assertArrayEquals( fmtx[ 1 ], f[ 1 ], 1e-9f );

			assertArrayEquals( dmtx[ 0 ], d[ 0 ], 1e-9 );
			assertArrayEquals( dmtx[ 1 ], d[ 1 ], 1e-9 );

			assertArrayEquals( bmtx[ 0 ], b[ 0 ] );
			assertArrayEquals( bmtx[ 1 ], b[ 1 ] );

			assertArrayEquals( imtx[ 0 ], i[ 0 ] );
			assertArrayEquals( imtx[ 1 ], i[ 1 ] );

			assertArrayEquals( lmtx[ 0 ], l[ 0 ] );
			assertArrayEquals( lmtx[ 1 ], l[ 1 ] );

		}
	}

	@Override
	@Test
	public void testWriterSeparation() {

		try (N5HDF5Writer writer1 = (N5HDF5Writer)createTempN5Writer()) {
			try (N5HDF5Writer writer2 = (N5HDF5Writer)createTempN5Writer()) {

				assertTrue(writer1.exists("/"));
				assertTrue(writer2.exists("/"));

				assertTrue(writer1.remove());
				assertTrue(writer2.exists("/"));

				assertTrue(writer2.remove());
			}
		}
	}
}
