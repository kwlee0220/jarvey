package jarvey.support;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import jarvey.datasource.DatasetOperationException;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialRowContext {
	private final JarveySchema m_schema;
	private final JarveyDataType[] m_colTypes;
	private @Nullable transient WKBReader m_reader = null;
	private @Nullable transient WKBWriter m_writer = null;
	
	public static SpatialRowContext of(JarveySchema schema) {
		return new SpatialRowContext(schema);
	}
	
	private SpatialRowContext(JarveySchema schema) {
		m_schema = schema;
		m_colTypes = FStream.from(m_schema.getColumnAll())
							.map(JarveyColumn::getJarveyDataType)
							.toArray(JarveyDataType.class);
		m_reader = new WKBReader(GeoUtils.GEOM_FACT);
		m_writer = new WKBWriter();
	}
	
	public JarveySchema getSchema() {
		return m_schema;
	}
	
	public int getColumnCount() {
		return m_colTypes.length;
	}
	
	public Object serialize(Object value, int idx) {
		JarveyDataType jtype = m_colTypes[idx];
		if ( jtype.isGeometryType() ) {
			final WKBReader reader = getWKBReader();
			
			byte[] wkb = (byte[])value;
			try {
				return (wkb != null) ? reader.read(wkb) : null;
			}
			catch ( ParseException e ) {
				throw new DatasetOperationException(e);
			}
		}
		else {
			return jtype.deserialize(value);
		}
	}
	
	public Object deserialize(Object value, int idx) {
		JarveyDataType jtype = m_colTypes[idx];
		if ( jtype.isGeometryType() ) {
			final WKBWriter writer = getWKBWriter();
			if ( value != null ) {
				return writer.write((Geometry)value);
			}
		}
		return jtype.deserialize(value);
	}
	
	private WKBReader getWKBReader() {
		return (m_reader != null) ? m_reader : new WKBReader(GeoUtils.GEOM_FACT);
	}
	
	private WKBWriter getWKBWriter() {
		return (m_writer != null) ? m_writer : new WKBWriter();
	}
}
