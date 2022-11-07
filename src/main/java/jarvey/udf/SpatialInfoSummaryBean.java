package jarvey.udf;

import org.locationtech.jts.geom.Envelope;

import jarvey.type.JarveyDataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialInfoSummaryBean {
	long m_count;
	long m_emptyGeomCount;
	Envelope m_bounds;
	
	public SpatialInfoSummaryBean() {
		m_count = 0;
		m_emptyGeomCount = 0;
		m_bounds = new Envelope();
	}
	
	public long getRecordCount() {
		return m_count;
	}
	
	public void setRecordCount(long count) {
		m_count = count;
	}
	
	public long getEmptyGeomCount() {
		return m_emptyGeomCount;
	}
	
	public void setEmptyGeomCount(long count) {
		m_emptyGeomCount = count;
	}
	
	public Double[] getBoundsCoordinates() {
		return JarveyDataTypes.Envelope_Type.serialize(m_bounds);
	}
	
	public void setBoundsCoordinates(Double[] coords) {
		m_bounds = JarveyDataTypes.Envelope_Type.deserialize(coords);
	}
}
