package jarvey.cluster;

import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class EstimateQuadSpacesOptions {
	public static final long DEFAULT_CLUSTER_BYTES = ClusterDatasetOptions.DEFAULT_PARTITION_SIZE;
	private static final long DEFAULT_UPPER_LIMIT = ClusterDatasetOptions.DEFAULT_PARTITION_LIMIT;
	private static final long DEFAULT_OUTLIER_LIMIT = ClusterDatasetOptions.DEFAULT_OUTLIER_LIMIT;

	private final double m_sampleRatio;
	private final long m_clusterSizeHint;
	private final long m_clusterSizeLimit;
	private final long m_outlierSizeLimit;
	private final boolean m_saveFlag;						// create file
	
	public static EstimateQuadSpacesOptions create() {
		return new EstimateQuadSpacesOptions(ClusterDatasetOptions.DEFAULT_SAMPLE_RATIO, DEFAULT_CLUSTER_BYTES,
											DEFAULT_UPPER_LIMIT, DEFAULT_OUTLIER_LIMIT, false);
	}
	
	public EstimateQuadSpacesOptions(double sampleRatio, long clusterSizeHint, long clusterSizeLimit,
									long outlierSizeLimit, boolean save) {
		Utilities.checkArgument(sampleRatio > 0 && sampleRatio <= 1,
								String.format("invalid sampling ratio: %.3f", sampleRatio));
		Utilities.checkArgument(clusterSizeHint >= 1, String.format("invalid cluster size: %d", clusterSizeHint));
		Utilities.checkArgument(outlierSizeLimit < clusterSizeHint,
								String.format("outlier(%s) < cluster_size(%s)",
												UnitUtils.toByteSizeString(outlierSizeLimit),
												UnitUtils.toByteSizeString(clusterSizeHint)));
		
		m_clusterSizeHint = clusterSizeHint;
		m_clusterSizeLimit = clusterSizeLimit;
		m_outlierSizeLimit = outlierSizeLimit;
		m_sampleRatio = sampleRatio;
		m_saveFlag = save;
	}
	
	public boolean isValid() {
		return m_clusterSizeHint > 0
				&& ((m_clusterSizeLimit == -1) || (m_clusterSizeHint <= m_clusterSizeLimit))
				&& m_sampleRatio > 0 && m_sampleRatio <= 1;
	}

	public double sampleRatio() {
		return m_sampleRatio;
	}
	public EstimateQuadSpacesOptions sampleRatio(double ratio) {
		return new EstimateQuadSpacesOptions(ratio, m_clusterSizeHint, m_clusterSizeLimit,
												m_outlierSizeLimit, m_saveFlag);
	}

	public long clusterSizeHint() {
		return m_clusterSizeHint;
	}
	public EstimateQuadSpacesOptions clusterSizeHint(long size) {
		return new EstimateQuadSpacesOptions(m_sampleRatio, size, m_clusterSizeLimit, m_outlierSizeLimit,
											m_saveFlag);
	}
	public EstimateQuadSpacesOptions clusterSizeHint(String sizeStr) {
		return clusterSizeHint((int)UnitUtils.parseByteSize(sizeStr));
	}
	
	public FOption<Long> clusterSizeLimit() {
		return m_clusterSizeLimit > 0 ? FOption.of(m_clusterSizeLimit) : FOption.empty();
	}
	public EstimateQuadSpacesOptions clusterSizeLimit(long nbytes) {
		return new EstimateQuadSpacesOptions(m_sampleRatio, m_clusterSizeHint, nbytes, m_outlierSizeLimit,
											m_saveFlag);
	}
	public EstimateQuadSpacesOptions clusterSizeLimit(String nbytesStr) {
		return clusterSizeLimit(UnitUtils.parseByteSize(nbytesStr));
	}
	
	public long outlierSizeLimit() {
		return m_outlierSizeLimit;
	}
	public EstimateQuadSpacesOptions outlierSizeLimit(long nbytes) {
		return new EstimateQuadSpacesOptions(m_sampleRatio, m_clusterSizeHint, m_clusterSizeLimit, nbytes,
											m_saveFlag);
	}
	public EstimateQuadSpacesOptions outlierSizeLimit(String nbytesStr) {
		return outlierSizeLimit(UnitUtils.parseByteSize(nbytesStr));
	}

	public boolean save() {
		return m_saveFlag;
	}
	public EstimateQuadSpacesOptions save(boolean flag) {
		return new EstimateQuadSpacesOptions(m_sampleRatio, m_clusterSizeHint, m_clusterSizeLimit,
											m_outlierSizeLimit, m_saveFlag);
	}
	
	@Override
	public String toString() {
		return String.format("ratio=%.2f, size={%s:%s:%s}, save=%s",
							m_sampleRatio, UnitUtils.toByteSizeString(m_outlierSizeLimit),
							UnitUtils.toByteSizeString(m_clusterSizeHint),
							UnitUtils.toByteSizeString(m_clusterSizeLimit), m_saveFlag);
	}
}