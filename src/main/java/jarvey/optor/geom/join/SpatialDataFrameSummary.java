package jarvey.optor.geom.join;

import org.locationtech.jts.geom.Envelope;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDataFrameSummary {
	private long m_recordCount;
	private long m_emptyGeomCount;
	private Envelope m_bounds;
	
	public SpatialDataFrameSummary(long recordCount, long emptyGeomCount, Envelope bounds) {
		this.m_recordCount = recordCount;
		this.m_emptyGeomCount = emptyGeomCount;
		this.m_bounds = bounds;
	}
	
	public long getRecordCount() {
		return m_recordCount;
	}
	
	public long getEmptyGeometryCount() {
		return m_emptyGeomCount;
	}
	
	public Envelope getBounds() {
		return m_bounds;
	}
	
	@Override
	public String toString() {
		return String.format("{count=%d, empty_geoms=%d, bounds=%s}", m_recordCount, m_emptyGeomCount, m_bounds);
	}
}
