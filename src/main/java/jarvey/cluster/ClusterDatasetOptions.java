package jarvey.cluster;

import utils.UnitUtils;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class ClusterDatasetOptions {
	public static final long DEFAULT_CLUSTER_SIZE = EstimateQuadKeysOptions.DEFAULT_CLUSTER_SIZE;
	public static final double DEFAULT_SAMPLE_RATIO = EstimateQuadKeysOptions.DEFAULT_SAMPLE_RATIO;
	
	private final long m_clusterSizeHint;
	private final double m_sampleRatio;
	private final Long[] m_candidateQuadIds;
	private final boolean m_force;						// create file
	
	public static final ClusterDatasetOptions FORCE = create().force(true);
	
	private ClusterDatasetOptions(boolean force, long clusterSizeHint, double sampleRatio,
									Long[] candidateQuadIds) {
		m_force = force;
		m_clusterSizeHint = clusterSizeHint;
		m_sampleRatio = sampleRatio;
		m_candidateQuadIds = candidateQuadIds;
	}
	
	public static ClusterDatasetOptions create() {
		return new ClusterDatasetOptions(false, DEFAULT_CLUSTER_SIZE, DEFAULT_SAMPLE_RATIO, null);
	}
	
	public EstimateQuadKeysOptions toEstimateQuadKeysOptions() {
		return EstimateQuadKeysOptions.create()
										.clusterSizeHint(m_clusterSizeHint)
										.sampleRatio(m_sampleRatio);
	}

	public boolean force() {
		return m_force;
	}
	public ClusterDatasetOptions force(boolean flag) {
		return new ClusterDatasetOptions(flag, m_clusterSizeHint, m_sampleRatio, m_candidateQuadIds);
	}

	public long clusterSizeHint() {
		return m_clusterSizeHint;
	}
	public ClusterDatasetOptions clusterSizeHint(long size) {
		return new ClusterDatasetOptions(m_force, size, m_sampleRatio, m_candidateQuadIds);
	}
	public ClusterDatasetOptions clusterSizeHint(String sizeStr) {
		long size = UnitUtils.parseByteSize(sizeStr);
		return new ClusterDatasetOptions(m_force, size, m_sampleRatio, m_candidateQuadIds);
	}

	public double sampleRatio() {
		return m_sampleRatio;
	}
	public ClusterDatasetOptions sampleRatio(double ratio) {
		return new ClusterDatasetOptions(m_force, m_clusterSizeHint, ratio, m_candidateQuadIds);
	}
	
	public Long[] candidateQuadIds() {
		return m_candidateQuadIds;
	}
	public ClusterDatasetOptions candidateQuadIds(Long[] quadIds) {
		return new ClusterDatasetOptions(m_force, m_clusterSizeHint, m_sampleRatio, quadIds);
	}
}