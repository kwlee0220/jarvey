package jarvey.type.temporal;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class TemporalPointIndexBean {
	public static final Encoder<TemporalPointIndexBean> ENCODER = Encoders.bean(TemporalPointIndexBean.class);

	private int m_id;
	private int m_segment;
	private long m_firstTs;
	private long m_lastTs;
	
	public TemporalPointIndexBean(int id, int segment, long firstTs, long lastTs) {
		m_id = id;
		m_segment = segment;
		m_firstTs = firstTs;
		m_lastTs = lastTs;
	}
	
	public int getId() {
		return m_id;
	}
	public void setId(int id) {
		m_id = id;
	}
	
	public int getSegment() {
		return m_segment;
	}
	public void setSegment(int segment) {
		m_segment = segment;
	}
	
	public long getFirstTs() {
		return m_firstTs;
	}
	public void setFirstTs(long ts) {
		m_firstTs = ts;
	}
	
	public long getLastTs() {
		return m_lastTs;
	}
	public void setLastTs(long ts) {
		m_lastTs = ts;
	}
	
	public String toStringExpr() {
		return String.format("%d:%d:%d:%d", m_id, m_segment, m_firstTs, m_lastTs);
	}
}
