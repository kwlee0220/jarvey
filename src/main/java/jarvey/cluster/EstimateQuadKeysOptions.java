package jarvey.cluster;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class EstimateQuadKeysOptions {
	public static final long DEFAULT_CLUSTER_SIZE = (128)*1024*1024;
	public static final double DEFAULT_SAMPLE_RATIO = 0.1;
	
	private final long m_clusterSizeHint;
	private final double m_sampleRatio;
	
	private EstimateQuadKeysOptions(long clusterSizeHint, double sampleRatio) {
		m_clusterSizeHint = clusterSizeHint;
		m_sampleRatio = sampleRatio;
	}
	
	public static EstimateQuadKeysOptions create() {
		return new EstimateQuadKeysOptions(DEFAULT_CLUSTER_SIZE, DEFAULT_SAMPLE_RATIO);
	}

	public long clusterSizeHint() {
		return m_clusterSizeHint;
	}
	public EstimateQuadKeysOptions clusterSizeHint(long size) {
		return new EstimateQuadKeysOptions(size, m_sampleRatio);
	}

	public double sampleRatio() {
		return m_sampleRatio;
	}
	public EstimateQuadKeysOptions sampleRatio(double ratio) {
		return new EstimateQuadKeysOptions(m_clusterSizeHint, ratio);
	}
}