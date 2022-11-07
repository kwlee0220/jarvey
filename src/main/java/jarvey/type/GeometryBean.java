package jarvey.type;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import jarvey.JarveyRuntimeException;

import utils.geo.util.GeoClientUtils;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryBean implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final GeometryBean EMPTY = new GeometryBean(null, null);

	public static final DataType DATA_TYPE = DataTypes.BinaryType;
	public static final Encoder<GeometryBean> ENCODER = Encoders.bean(GeometryBean.class);

	private byte[] m_wkb;
	private transient Geometry m_geom = null;

	public static GeometryBean empty() {
		return EMPTY;
	}
	
	public static GeometryBean of(Geometry geom) {
		return new GeometryBean(geom, null);
	}
	
	public static GeometryBean fromWkb(byte[] wkb) {
		return new GeometryBean(null, wkb);
	}
	
	public GeometryBean() {
		this(null, null);
	}
	
	private GeometryBean(Geometry geom, byte[] wkb) {
		m_geom = geom;
		m_wkb = wkb;
	}
	
	public byte[] getWkb() {
		if ( m_wkb == null ) {
			m_wkb = GeoClientUtils.toWKB(m_geom);
		}
		return m_wkb;
	}
	
	public void setWkb(byte[] wkb) {
		m_wkb = wkb;
		m_geom = null;
	}
	
	public Geometry asGeometry() {
		if ( m_geom == null ) {
			m_geom = parseWkb(m_wkb);
		}
		
		return m_geom;
	}
	
	public void update(Geometry geom) {
		m_geom = geom;
		m_wkb = null;
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
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final byte[] m_wkb;
		
		private SerializationProxy(GeometryBean bean) {
			m_wkb = bean.getWkb();
		}
		
		private Object readResolve() {
			return GeometryBean.fromWkb(m_wkb);
		}
	}
}
