package org.janelia.saalfeldlab.n5.hdf5;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;

public class HDF5DatasetAttributes extends DatasetAttributes {

	private HDF5DatasetAttributes(long[] dimensions, int[] outerBlockSize, DataType dataType, BlockCodecInfo blockCodecInfo, DataCodecInfo[] dataCodecInfos) {

		super(dimensions, outerBlockSize, dataType, blockCodecInfo, dataCodecInfos);
	}

	HDF5DatasetAttributes(long[] dimensions, int[] blockSize, DataType dataType, Compression compression) {
		this( dimensions, blockSize, dataType, null, new DataCodecInfo[]{compression});
	}

}
