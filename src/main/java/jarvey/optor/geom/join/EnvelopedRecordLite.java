package jarvey.optor.geom.join;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.quadtree.Enveloped;
import jarvey.support.RecordLite;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class EnvelopedRecordLite implements Enveloped, GeometryHolder {
	private final RecordLite m_rec;
	private final Geometry m_geom;
	private final Envelope m_envl4326;
	
	EnvelopedRecordLite(RecordLite rec, Geometry geom, Envelope envl4326) {
		m_rec = rec;
		m_geom = geom;
		m_envl4326 = envl4326;
	}
	
	public RecordLite getRecord() {
		return m_rec;
	}
	
	@Override
	public Geometry getGeometry() {
		return m_geom;
	}

	@Override
	public Envelope getEnvelope84() {
		return m_envl4326;
	}
}