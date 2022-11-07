package jarvey.cluster;

import utils.UnitUtils;
import utils.func.FOption;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class ClusterDatasetOptions {
	public static final double DEFAULT_SAMPLE_RATIO = 0.3f;
	public static final long DEFAULT_PARTITION_SIZE = UnitUtils.parseByteSize("64mb");
	public static final long DEFAULT_PARTITION_LIMIT = -1L;
	public static final long DEFAULT_OUTLIER_LIMIT = UnitUtils.parseByteSize("2mb");
	public static final boolean DEFAULT_DROP_OUTLIERS = true;
	
	private final long[] m_candidateQuadIds;
	private final double m_sampleRatio;
	private final long m_clusterSizeHint;
	private final long m_clusterSizeLimit;
	private final long m_outlierSizeLimit;
	private final boolean m_dropFinalOutliers;
	private final boolean m_force;						// create file
	
	public static final ClusterDatasetOptions FORCE = create().force(true);
	
	private ClusterDatasetOptions(long[] candidateQuadIds, double sampleRatio,
									long clusterSizeHint, long clusterSizeLimit, long outlierLimit,
									boolean dropOutliers, boolean force) {
		m_candidateQuadIds = candidateQuadIds;
		m_sampleRatio = sampleRatio;
		m_clusterSizeHint = clusterSizeHint;
		m_clusterSizeLimit = clusterSizeLimit;
		m_outlierSizeLimit = outlierLimit;
		m_dropFinalOutliers = dropOutliers;
		m_force = force;
	}
	
	public static ClusterDatasetOptions create() {
		return new ClusterDatasetOptions(null, DEFAULT_SAMPLE_RATIO,
										DEFAULT_PARTITION_SIZE, DEFAULT_PARTITION_LIMIT, DEFAULT_OUTLIER_LIMIT,
										DEFAULT_DROP_OUTLIERS, false);
	}
	
	public boolean isValid() {
		if ( m_candidateQuadIds != null ) {
			return m_candidateQuadIds.length > 0;
		}
		
		return m_clusterSizeHint > 0
				&& ((m_clusterSizeLimit == -1) || (m_clusterSizeHint <= m_clusterSizeLimit))
				&& m_sampleRatio > 0 && m_sampleRatio <= 1;
	}

	public double sampleRatio() {
		return m_sampleRatio;
	}
	public ClusterDatasetOptions sampleRatio(double ratio) {
		return new ClusterDatasetOptions(m_candidateQuadIds, ratio, m_clusterSizeHint,
										m_clusterSizeLimit, m_outlierSizeLimit, m_dropFinalOutliers, m_force);
	}
	
	public long clusterSizeHint() {
		return m_clusterSizeHint;
	}
	public ClusterDatasetOptions clusterSizeHint(long nbytes) {
		return new ClusterDatasetOptions(m_candidateQuadIds, m_sampleRatio, nbytes,
										m_clusterSizeLimit, m_outlierSizeLimit, m_dropFinalOutliers, m_force);
	}
	public ClusterDatasetOptions clusterSizeHint(String nbytesStr) {
		return clusterSizeHint(UnitUtils.parseByteSize(nbytesStr));
	}
	
	public FOption<Long> clusterSizeLimit() {
		return m_clusterSizeLimit > 0 ? FOption.of(m_clusterSizeLimit) : FOption.empty();
	}
	public ClusterDatasetOptions clusterSizeLimit(long nbytes) {
		return new ClusterDatasetOptions(m_candidateQuadIds, m_sampleRatio, m_clusterSizeHint,
										nbytes, m_outlierSizeLimit, m_dropFinalOutliers, m_force);
	}
	public ClusterDatasetOptions clusterSizeLimit(String nbytesStr) {
		return clusterSizeLimit(UnitUtils.parseByteSize(nbytesStr));
	}
	
	public long outlierSizeLimit() {
		return m_outlierSizeLimit;
	}
	public ClusterDatasetOptions outlierSizeLimit(long nbytes) {
		return new ClusterDatasetOptions(m_candidateQuadIds, m_sampleRatio, m_clusterSizeHint,
										m_clusterSizeLimit, nbytes, m_dropFinalOutliers, m_force);
	}
	public ClusterDatasetOptions outlierSizeLimit(String nbytesStr) {
		return outlierSizeLimit(UnitUtils.parseByteSize(nbytesStr));
	}
	
	public long[] candidateQuadIds() {
		return m_candidateQuadIds;
	}
	public ClusterDatasetOptions candidateQuadIds(long[] quadIds) {
		return new ClusterDatasetOptions(quadIds, m_sampleRatio, m_clusterSizeHint,
										m_clusterSizeLimit, m_outlierSizeLimit, m_dropFinalOutliers, m_force);
	}

	public boolean dropFinalOutliers() {
		return m_dropFinalOutliers;
	}
	public ClusterDatasetOptions dropFinalOutliers(boolean flag) {
		return new ClusterDatasetOptions(m_candidateQuadIds, m_sampleRatio, m_clusterSizeHint,
										m_clusterSizeLimit, m_outlierSizeLimit, flag, m_force);
	}

	public boolean force() {
		return m_force;
	}
	public ClusterDatasetOptions force(boolean flag) {
		return new ClusterDatasetOptions(m_candidateQuadIds, m_sampleRatio, m_clusterSizeHint,
										m_clusterSizeLimit, m_outlierSizeLimit, m_dropFinalOutliers, flag);
	}
	
	public EstimateQuadSpacesOptions toEstimateQuadKeysOptions() {
		return EstimateQuadSpacesOptions.create()
										.sampleRatio(m_sampleRatio)
										.clusterSizeHint(m_clusterSizeHint)
										.clusterSizeLimit(m_clusterSizeLimit)
										.outlierSizeLimit(m_outlierSizeLimit)
										.save(true);
	}
	
	@Override
	public String toString() {
		return String.format("ratio=%.2f, size={%s:%s:%s}, drop_outlier=%s, force=%s",
							m_sampleRatio, UnitUtils.toByteSizeString(m_outlierSizeLimit),
							UnitUtils.toByteSizeString(m_clusterSizeHint),
							UnitUtils.toByteSizeString(m_clusterSizeLimit),
							m_dropFinalOutliers, m_force);
	}
}