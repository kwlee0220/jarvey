package jarvey.quadtree;

import org.locationtech.jts.geom.Envelope;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Enveloped {
	public Envelope getEnvelope84();
	
	public default boolean intersects(Envelope envl) {
		return getEnvelope84().intersects(envl);
	}
	
	public default boolean intersects(Enveloped v) {
		return getEnvelope84().intersects(v.getEnvelope84());
	}
	
	public default boolean contains(Enveloped v) {
		return getEnvelope84().contains(v.getEnvelope84());
	}
	
	public default boolean containedBy(Enveloped v) {
		return v.getEnvelope84().contains(getEnvelope84());
	}
}
