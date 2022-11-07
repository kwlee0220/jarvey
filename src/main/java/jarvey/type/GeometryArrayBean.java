package jarvey.type;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import jarvey.datasource.DatasetOperationException;

import utils.geo.util.GeoClientUtils;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryArrayBean {
	public static final Encoder<GeometryArrayBean> ENCODER = Encoders.bean(GeometryArrayBean.class);
	private static final GeometryType SERDE = JarveyDataTypes.Geometry_Type;
	
	private byte[][] m_wkbs;
	
	public GeometryArrayBean() {
		m_wkbs = new byte[0][];
	}
	
	public GeometryArrayBean(byte[][] wkbs) {
		m_wkbs = wkbs;
	}
	
	public int length() {
		return m_wkbs.length;
	}
	
	public byte[][] getWkbs() {
		return m_wkbs;
	}
	
	public void setWkbs(byte[][] wkbs) {
		m_wkbs = wkbs;
	}
	
	public void add(byte[] wkb) {
		byte[][] wkbs = new byte[m_wkbs.length + 1][];
		System.arraycopy(m_wkbs, 0, wkbs, 0, m_wkbs.length);
		wkbs[m_wkbs.length] = wkb;
		m_wkbs = wkbs;
	}
	
	public void addAll(byte[][] wkbs) {
		byte[][] concated = new byte[m_wkbs.length + wkbs.length][];
		System.arraycopy(m_wkbs, 0, concated, 0, m_wkbs.length);
		System.arraycopy(wkbs, 0, concated, m_wkbs.length, wkbs.length);
		m_wkbs = concated;
	}
	
	public Geometry[] asGeometries() {
		try {
			Geometry[] geoms = new Geometry[m_wkbs.length];
			for ( int i =0; i < geoms.length; ++i ) {
				geoms[i] = GeoClientUtils.fromWKB(m_wkbs[i]);
			}
			
			return geoms;
		}
		catch ( ParseException e ) {
			throw new DatasetOperationException(e);
		}
	}
	
	public void update(Geometry[] geoms) {
		m_wkbs = new byte[geoms.length][];
		for ( int i =0; i < geoms.length; ++i ) {
			m_wkbs[i] = SERDE.serialize(geoms[i]);
		}
	}

/*
	public static final DataType DATA_TYPE = DataTypes.createArrayType(DataTypes.BinaryType);
	public static final StructType ROW_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("wkb_array", (DataType)DataTypes.createArrayType(DataTypes.BinaryType), true),
	});
	
	public static final GeometryArrayBean fromSpark(Object sparkValue) {
		if ( sparkValue == null ) {
			return null;
		}
		else if ( sparkValue instanceof byte[][] ) {
			return new GeometryArrayBean((byte[][])sparkValue);
		}
		else if ( sparkValue instanceof WrappedArray ) {
			WrappedArray<byte[]> wkbArray = (WrappedArray<byte[]>)sparkValue;
			
			byte[][] wkbs = new byte[wkbArray.length()][];
			for ( int i =0; i < wkbs.length; ++i ) {
				wkbs[i] = (byte[])wkbArray.apply(i);
			}
			return new GeometryArrayBean(wkbs);
		}
		else {
			throw new AssertionError("invalid GeometryArrayValue: " + sparkValue);
		}
	}
	
	private GeometryArrayBean(byte[][] wkbs) {
		m_wkbs = wkbs;
	}
	
	public byte[][] toSparkValue() {
		return m_wkbs;
	}
*/
}
