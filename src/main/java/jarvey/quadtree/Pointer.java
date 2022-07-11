package jarvey.quadtree;

import org.locationtech.jts.geom.Envelope;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class Pointer implements Enveloped, Comparable<Pointer> {
	private Envelope m_envl;
	private int m_index = -1;
	
	public Pointer(Envelope envl, int index) {
		m_envl = envl;
		m_index = index;
	}
	
	public Pointer() { }

	@Override
	public Envelope getEnvelope84() {
		return m_envl;
	}
	
	public int index() {
		return m_index;
	}
	
	public boolean isNull() {
		return m_index < 0;
	}
	
	@Override
	public String toString() {
		return String.format("%d:%s", m_index, m_envl);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof Pointer) ) {
			return false;
		}
		
		Pointer other = (Pointer)obj;
		return m_index == other.m_index;
	}
	
	@Override
	public int hashCode() {
		return m_index;
	}

	@Override
	public int compareTo(Pointer o) {
		return m_index - o.m_index;
	}
}