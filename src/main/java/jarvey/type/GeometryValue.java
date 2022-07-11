/**
 * 
 */
package jarvey.type;

import java.io.Serializable;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import jarvey.JarveyRuntimeException;
import utils.func.Lazy;
import utils.geo.util.GeoClientUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryValue implements Serializable {
	private static final long serialVersionUID = 1L;

	private final byte[] m_wkb;
	private transient final Lazy<Geometry> m_lazyGeom;
	
	public GeometryValue(Geometry geom) {
		m_wkb = GeoClientUtils.toWKB(geom);
		m_lazyGeom = Lazy.of(geom);
	}
	
	private GeometryValue(byte[] wkb) {
	    m_wkb = wkb;
	    m_lazyGeom = Lazy.of(() -> parseWkb(m_wkb));
	}
	
	public Geometry getGeometry() {
		return m_wkb != null ? m_lazyGeom.get() : null;
	}
	
	public byte[] getWkb() {
		return m_wkb;
	}
	
	public static GeometryValue from(byte[] wkb) {
		return wkb != null ? new GeometryValue(wkb) : null;
	}
	
	public static byte[] serialize(Geometry geom) {
		return geom != null ? GeoClientUtils.toWKB(geom) : null;
	}
	
	public static Geometry deserialize(byte[] wkb) {
		return (wkb != null) ? parseWkb(wkb) : null;
	}
	
	private static Geometry parseWkb(byte[] wkb) {
		try {
			return GeoClientUtils.fromWKB(wkb);
		}
		catch ( ParseException e ) {
			throw new JarveyRuntimeException("fails to parse WKB, cause=" + e);
		}
	}
}
