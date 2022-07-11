/**
 * 
 */
package jarvey.type2;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import jarvey.datasource.DatasetOperationException;
import scala.collection.mutable.WrappedArray;
import utils.geo.util.GeoClientUtils;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeometryArrayValue implements Serializable {
	private static final long serialVersionUID = 1L;

	private byte[][] m_wkbs;

	public static final DataType DATA_TYPE = DataTypes.createArrayType(DataTypes.BinaryType);
	public static final StructType ROW_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("wkb_array", DataTypes.BinaryType, true),
	});
	
	public GeometryArrayValue(byte[][] wkbs) {
		m_wkbs = wkbs;
	}
	
	public GeometryArrayValue() {
		m_wkbs = new byte[0][];
	}
	
	public Geometry[] getGeometries() {
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
	
	public GeometryArrayValue add(byte[] wkb) {
		byte[][] wkbs = new byte[m_wkbs.length + 1][];
		System.arraycopy(m_wkbs, 0, wkbs, 0, m_wkbs.length);
		wkbs[m_wkbs.length] = wkb;
		
		return new GeometryArrayValue(wkbs);
	}
	
	public GeometryArrayValue addAll(byte[][] wkbs) {
		byte[][] concated = new byte[m_wkbs.length + wkbs.length][];
		System.arraycopy(m_wkbs, 0, concated, 0, m_wkbs.length);
		System.arraycopy(wkbs, 0, concated, m_wkbs.length, wkbs.length);

		return new GeometryArrayValue(concated);
	}
	
	public static GeometryArrayValue concat(GeometryArrayValue rows1, GeometryArrayValue rows2) {
		return rows1.addAll(rows2.m_wkbs);
	}
	
	public static GeometryArrayValue from(WrappedArray<byte[]> wkbArray) {
		if ( wkbArray == null ) {
			return null;
		}
		else {
			byte[][] wkbs = new byte[wkbArray.length()][];
			for ( int i =0; i < wkbs.length; ++i ) {
				wkbs[i] = (byte[])wkbArray.apply(i);
			}
			
			return new GeometryArrayValue(wkbs);
		}
	}
}
