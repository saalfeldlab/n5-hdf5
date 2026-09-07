package org.janelia.saalfeldlab.n5.hdf5;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.janelia.saalfeldlab.n5.ContainerDialect;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.N5Dialect;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.janelia.saalfeldlab.n5.N5KeyValueReader.ATTRIBUTES_JSON;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Hdf5Dialect} with {@link Hdf5HierarchyStore}.
 * <p>
 * N5 semantics on HDF5 are covered by {@link N5HDF5Test} (through {@code
 * AbstractN5Test}). What is tested here is what the old reader/writer do not
 * have: the virtual "attributes.json" that the store synthesizes for generic
 * {@link HierarchyStore} consumers, and that a plain {@link N5Dialect} on that
 * view sees an N5-shaped container.
 */
public class Hdf5DialectTest {

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private static final long[] dimensions = {10, 20, 30};

	private static final int[] blockSize = {5, 5, 5};

	private static final N5DirectoryPath GROUP = N5DirectoryPath.of("group");

	private static final N5DirectoryPath DATASET = N5DirectoryPath.of("group/data");

	private N5HDF5Writer createContainer() throws IOException {

		final N5HDF5Writer h5 = new N5HDF5Writer(new File(tempDir.getRoot(), "test.hdf5").getCanonicalPath());
		h5.createGroup("group");
		h5.setAttribute("group", "int", 42);
		h5.setAttribute("group", "string", "value");
		h5.setAttribute("group", "nested/key", "structured");
		h5.createDataset("group/data", new DatasetAttributes(dimensions, blockSize, DataType.UINT8, new RawCompression()));
		return h5;
	}

	@Test
	public void testAttributesJsonView() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			final HierarchyStore store = h5.getContainerDialect().getHierarchyStore();
			final Gson gson = h5.getContainerDialect().getGson();

			/* native attributes and the N5_JSON_ROOT tree are merged into one view */
			final JsonObject group = store
					.readAttributesJson(GROUP, ATTRIBUTES_JSON, gson)
					.getAsJsonObject();
			assertEquals(42, group.get("int").getAsInt());
			assertEquals("value", group.get("string").getAsString());
			assertEquals("structured", group.getAsJsonObject("nested").get("key").getAsString());

			/* the derived dataset attributes are part of the view of a dataset */
			final JsonObject dataset = store
					.readAttributesJson(DATASET, ATTRIBUTES_JSON, gson)
					.getAsJsonObject();
			assertArrayEquals(dimensions, gson.fromJson(dataset.get("dimensions"), long[].class));
			assertArrayEquals(blockSize, gson.fromJson(dataset.get("blockSize"), int[].class));
			assertEquals(DataType.UINT8, gson.fromJson(dataset.get("dataType"), DataType.class));
			assertTrue(dataset.has("compression"));
		}
	}

	/**
	 * A hierarchy harvested from the store is N5-shaped, so a plain {@link
	 * N5Dialect} over it resolves groups, datasets, and attributes. This is what
	 * {@link Hdf5Dialect#withStore} falls back to for a foreign store, and what
	 * a translated (in-memory) hierarchy would be read through.
	 */
	@Test
	public void testN5DialectOverStore() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			final ContainerDialect dialect = h5.getContainerDialect();
			final ContainerDialect n5 = new N5Dialect(dialect.getHierarchyStore(), dialect.getGson());

			assertTrue(n5.groupExists(GROUP));
			assertTrue(n5.datasetExists(DATASET));
			assertArrayEquals(new String[]{"data"}, n5.list(GROUP));
			/* a dataset is a directory with no children, not an error */
			assertArrayEquals(new String[0], n5.list(DATASET));
			assertThrows(N5NoSuchKeyException.class, () -> n5.list(N5DirectoryPath.of("nope")));
			assertEquals("value", n5.getAttribute(GROUP, "string", String.class));
			assertArrayEquals(dimensions, n5.getDatasetAttributes(DATASET).getDimensions());
		}
	}

	/**
	 * The store is a read-only view: its {@code HierarchyStore} write methods
	 * throw rather than corrupt the container. Most importantly, a {@link
	 * N5Dialect} on it cannot create a "dataset" that is really a group with
	 * dataset attributes and no data behind it.
	 */
	@Test
	public void testStoreWritesThrow() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			final ContainerDialect dialect = h5.getContainerDialect();
			final HierarchyStore store = dialect.getHierarchyStore();
			final Gson gson = dialect.getGson();
			final N5DirectoryPath ghost = N5DirectoryPath.of("ghost");

			assertThrows(UnsupportedOperationException.class, () -> store.createDirectories(ghost));
			assertThrows(UnsupportedOperationException.class, () -> store.removeDirectory(GROUP));
			assertThrows(UnsupportedOperationException.class,
					() -> store.writeAttributesJson(GROUP, ATTRIBUTES_JSON, new JsonObject(), gson));

			final ContainerDialect n5 = new N5Dialect(store, gson);
			assertThrows(UnsupportedOperationException.class,
					() -> n5.createDataset(ghost, new DatasetAttributes(dimensions, blockSize, DataType.UINT8, new RawCompression())));
			assertThrows(UnsupportedOperationException.class, () -> n5.setAttribute(GROUP, "int", 7));

			/* nothing of the above changed the container */
			assertFalse(h5.exists("ghost"));
			assertEquals(42, (int)h5.getAttribute("group", "int", int.class));
			assertEquals(int.class, dialect.listAttributes(GROUP).get("int"));
		}
	}

	@Test
	public void testWithStoreKeepsHdf5Semantics() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			final ContainerDialect dialect = h5.getContainerDialect();
			final ContainerDialect copy = dialect.withStore(dialect.getHierarchyStore());

			assertTrue(copy instanceof Hdf5Dialect);
			/* HDF5 attribute types survive, where the json view would say long */
			assertEquals(int.class, copy.listAttributes(GROUP).get("int"));
		}
	}

	/**
	 * JHDF5 keeps metadata of its own in attributes named {@code __LIKE_THIS__}
	 * and hides them from {@code getAttributeNames}. They must stay invisible to
	 * both attribute views, and must survive setting the root attribute — which
	 * deletes every attribute it can see.
	 */
	@Test
	public void testInternalHdf5AttributesAreHidden() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			h5.writer.int32().setAttr("/group", "__INTERNAL__", 7);

			assertFalse(h5.listAttributes("group").containsKey("__INTERNAL__"));
			assertEquals(int.class, h5.listAttributes("group").get("int"));

			final JsonObject attributes = h5.getAttributes("group").getAsJsonObject();
			assertFalse(attributes.has("__INTERNAL__"));
			assertEquals(42, attributes.get("int").getAsInt());

			/* setting the root attribute replaces the user attributes, and only those */
			final Map<String, Object> replacement = new HashMap<>();
			replacement.put("only", "this");
			h5.setAttribute("group", "/", replacement);

			final JsonObject afterRoot = h5.getAttributes("group").getAsJsonObject();
			assertEquals("this", afterRoot.get("only").getAsString());
			assertFalse(afterRoot.has("int"));
			assertFalse(afterRoot.has("__INTERNAL__"));
			assertTrue(h5.writer.object().getAllAttributeNames("/group").contains("__INTERNAL__"));
		}
	}

	/**
	 * {@code a\/b} is the N5 spelling of an attribute <em>named</em> {@code a/b},
	 * and HDF5 attribute names are opaque strings, so that name is stored
	 * natively under its unescaped spelling. The same key read back returns it,
	 * and the unescaped {@code a/b} keeps its N5 meaning (the path {@code a} to
	 * {@code b}), so a name and a path of the same spelling coexist.
	 * <p>
	 * NB: {@code listAttributes} and the merged view report the literal
	 * (unescaped) name {@code a/b}, as N5 does for {@code {"a/b":1}} in an
	 * attributes.json -- feeding that back to {@code getAttribute} means the path.
	 * Escaping on output needs an escaping helper in n5 core.
	 */
	@Test
	public void testEscapedAttributeNamesAreNative() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			h5.setAttribute("group", "a\\/b", 5);

			assertTrue(h5.writer.object().getAttributeNames("/group").contains("a/b"));
			assertEquals(5, (int)h5.getAttribute("group", "a\\/b", int.class));
			/* the path a -> b is a different attribute, and does not exist yet */
			assertNull(h5.getAttribute("group", "a/b", int.class));

			h5.setAttribute("group", "a/b", 7);
			assertEquals(5, (int)h5.getAttribute("group", "a\\/b", int.class));
			assertEquals(7, (int)h5.getAttribute("group", "a/b", int.class));

			/* both are visible, under their literal names */
			final Map<String, Class<?>> classes = h5.listAttributes("group");
			assertEquals(int.class, classes.get("a/b"));
			assertTrue(classes.containsKey("a"));
			final JsonObject merged = h5.getAttributes("group").getAsJsonObject();
			assertEquals(5, merged.get("a/b").getAsInt());
			assertEquals(7, merged.getAsJsonObject("a").get("b").getAsInt());

			/* removing the name leaves the path alone */
			assertTrue(h5.removeAttribute("group", "a\\/b"));
			assertFalse(h5.writer.object().getAttributeNames("/group").contains("a/b"));
			assertNull(h5.getAttribute("group", "a\\/b", int.class));
			assertEquals(7, (int)h5.getAttribute("group", "a/b", int.class));
		}
	}

	/**
	 * The same for {@code \[}, which distinguishes the attribute named {@code
	 * c[0]} from element 0 of the array {@code c}. A backslash that escapes
	 * nothing is part of the name (N5 has no {@code \\} escape yet).
	 */
	@Test
	public void testEscapedArrayIndexAndBackslashInNames() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			h5.setAttribute("group", "c\\[0]", "x");
			h5.setAttribute("group", "c[0]", 3);

			assertTrue(h5.writer.object().getAttributeNames("/group").contains("c[0]"));
			assertEquals("x", h5.getAttribute("group", "c\\[0]", String.class));
			assertEquals(3, (int)h5.getAttribute("group", "c[0]", int.class));

			h5.setAttribute("group", "d\\b", 1);
			assertTrue(h5.writer.object().getAttributeNames("/group").contains("d\\b"));
			assertEquals(1, (int)h5.getAttribute("group", "d\\b", int.class));
		}
	}

	/**
	 * An attribute whose name contains {@code /} or {@code [} written by some
	 * other HDF5 tool is reachable, and removable, under its escaped N5 name.
	 * A key that is an attribute <em>path</em> never resolves to such a name.
	 */
	@Test
	public void testForeignNativeAttributeNames() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			h5.writer.int32().setAttr("/group", "a/b", 5);
			h5.writer.int32().setAttr("/group", "[0]", 6);

			assertEquals(5, (int)h5.getAttribute("group", "a\\/b", int.class));
			assertEquals(6, (int)h5.getAttribute("group", "\\[0]", int.class));
			/* paths, not names: a -> b, and element 0 of the root */
			assertNull(h5.getAttribute("group", "a/b", int.class));
			assertNull(h5.getAttribute("group", "[0]", int.class));

			assertTrue(h5.removeAttribute("group", "a\\/b"));
			assertFalse(h5.writer.object().getAttributeNames("/group").contains("a/b"));
		}
	}

	/**
	 * A value with no native HDF5 representation goes into the {@code
	 * N5_JSON_ROOT} tree, where {@code GsonUtils} unescapes the key the same way
	 * -- so both halves agree on which attribute a name denotes.
	 */
	@Test
	public void testEscapedAttributeNameFallsBackToJson() throws IOException {

		try (final N5HDF5Writer h5 = createContainer()) {

			final Map<String, Object> structured = new HashMap<>();
			structured.put("x", 1);
			h5.setAttribute("group", "a\\/b", structured);

			assertFalse(h5.writer.object().getAttributeNames("/group").contains("a/b"));
			assertEquals(1, h5.getAttribute("group", "a\\/b", JsonObject.class).get("x").getAsInt());
			assertEquals(1, h5.getAttribute("group", "a\\/b/x", int.class).intValue());
			assertTrue(h5.listAttributes("group").containsKey("a/b"));
			assertEquals(1, h5.getAttributes("group").getAsJsonObject()
					.getAsJsonObject("a/b").get("x").getAsInt());

			assertTrue(h5.removeAttribute("group", "a\\/b"));
			assertNull(h5.getAttribute("group", "a\\/b", JsonObject.class));
		}
	}
}
