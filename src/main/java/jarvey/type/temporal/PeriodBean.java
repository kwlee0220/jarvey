package jarvey.type.temporal;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
final class PeriodBean {
	private long m_firstTs;
	private long m_lastTs;
	
	PeriodBean(long firstTs, long lastTs) {
		m_firstTs = firstTs;
		m_lastTs = lastTs;
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
}
