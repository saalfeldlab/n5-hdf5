package org.janelia.saalfeldlab.n5.hdf5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.janelia.saalfeldlab.n5.ContainerDialect;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.N5Dialect;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;

import java.lang.reflect.Type;
import java.util.Map;

import static org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import static org.janelia.saalfeldlab.n5.hdf5.Hdf5HierarchyStore.hdf5Path;

/**
 * {@code ContainerDialect} for HDF5 files.
 * <ul>
 * <li>Groups and datasets are the directories of the hierarchy.</li>
 * <li>Attributes are native HDF5 attributes, with structured values stored as
 * json in a single {@code N5_JSON_ROOT} string attribute.</li>
 * <li>The mandatory dataset attributes are <em>derived</em> from the HDF5
 * dataset, not stored: datasets cannot be reshaped, and compression is always
 * reported as raw.</li>
 * </ul>
 * Attribute access goes to the native HDF5 attributes rather than through
 * {@link HierarchyStore#readAttributesJson}, so that HDF5 types survive: an
 * {@code int32} attribute reads back as an {@code int}, not as a json number.
 */
public class Hdf5Dialect implements ContainerDialect {

	private final Hdf5HierarchyStore store;

	private final Gson gson;

	Hdf5Dialect(final Hdf5HierarchyStore store, final Gson gson) {

		this.store = store;
		this.gson = gson;
	}

	/**
	 * NB: The returned {@code HierarchyStore} only implements the <em>read</em>
	 * half of the interface. The write-direction throws {@code
	 * UnsupportedOperationException}.
	 * <p>
	 * Internally, this is a {@code Hdf5HierarchyStore} and {@code Hdf5Dialect}
	 * uses its package-private methods to implement write operations. However,
	 * attribute writing cannot be exposed fully lossless through the {@code
	 * HierarchyStore} interface.
	 */
	@Override
	public HierarchyStore getHierarchyStore() {

		return store;
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * If the passed {@code store} is not a {@link Hdf5HierarchyStore} a {@link
	 * N5Dialect} is returned as a fall-back, keeping {@code Gson} of this
	 * dialect. (This is intended to work with n5-universe's {@code
	 * TranslatedN5Reader} which wraps the delegate store, losing the additional
	 * HDF5-specific methods of {@code Hdf5HierarchyStore} in the process.)
	 */
	@Override
	public ContainerDialect withStore(final HierarchyStore store) {

		return store instanceof Hdf5HierarchyStore
				? new Hdf5Dialect((Hdf5HierarchyStore)store, gson)
				: new N5Dialect(store, gson);
	}

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ READ:                                                                 │
	// └───────────────────────────────────────────────────────────────────────┘

	@Override
	public <T> T getAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Type type) throws N5Exception {

		return store.getAttribute(hdf5Path(path), attributePath, type, gson);
	}

	@Override
	public JsonElement getAttributes(final N5DirectoryPath path) throws N5IOException {

		return getAttribute(path, "/", JsonElement.class);
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final N5DirectoryPath path) throws N5IOException {

		return store.datasetAttributes(hdf5Path(path));
	}

	@Override
	public boolean datasetExists(final N5DirectoryPath path) throws N5IOException {

		return store.isDataSet(hdf5Path(path));
	}

	@Override
	public boolean groupExists(final N5DirectoryPath path) throws N5IOException {

		return store.isDirectory(path);
	}

	@Override
	public String[] list(final N5DirectoryPath path) throws N5IOException {

		return store.listDirectories(path);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * NB: For native attributes, classes reflect the actual HDF5 types, not
	 * those inferred from a json representation. An {@code int32} attribute is
	 * reported as {@code int.class}, where the json view would say {@code
	 * long.class}.
	 */
	@Override
	public Map<String, Class<?>> listAttributes(final N5DirectoryPath path) throws N5Exception {

		return store.attributeClasses(hdf5Path(path));
	}

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ WRITE:                                                                │
	// └───────────────────────────────────────────────────────────────────────┘

	@Override
	public <T> void setAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final T attribute) throws N5IOException {

		store.setAttribute(hdf5Path(path), attributePath, attribute, gson);
	}

	@Override
	public void setAttributes(
			final N5DirectoryPath path,
			final Map<String, ?> attributes) throws N5IOException {

		attributes.forEach((attributePath, attribute) -> setAttribute(path, attributePath, attribute));
	}

	@Override
	public boolean removeAttribute(
			final N5DirectoryPath path,
			final String attributePath) throws N5IOException {

		return store.removeAttribute(hdf5Path(path), attributePath, gson);
	}

	@Override
	public <T> T removeAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Class<T> clazz) throws N5Exception {

		return store.removeAttribute(hdf5Path(path), attributePath, clazz, gson);
	}

	@Override
	public void setDatasetAttributes(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		throw new UnsupportedOperationException("HDF5 datasets cannot be reshaped.");
	}

	@Override
	public void createGroup(final N5DirectoryPath path) throws N5IOException {

		store.createGroup(path);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * NB: An existing dataset at {@code path} is deleted and re-created.
	 */
	@Override
	public void createDataset(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		store.createDataset(hdf5Path(path), attributes);
	}

	@Override
	public boolean remove(final N5DirectoryPath path) throws N5IOException {

		store.delete(path);
		return !store.isDirectory(path);
	}
}
