package jarvey.optor;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import jarvey.datasource.DatasetOperationException;
import jarvey.support.GeoUtils;
import jarvey.support.RecordLite;
import jarvey.support.Rows;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveySchema;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSerDe implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final JarveySchema m_schema;
	private final JarveyDataType[] m_colTypes;
	private @Nullable transient WKBReader m_reader = null;
	private @Nullable transient WKBWriter m_writer = null;
	
	public static RecordSerDe of(JarveySchema schema) {
		return new RecordSerDe(schema);
	}
	
	private RecordSerDe(JarveySchema schema) {
		m_schema = schema;
		m_colTypes = FStream.from(m_schema.getColumnAll())
							.map(JarveyColumn::getJarveyDataType)
							.toArray(JarveyDataType.class);
		m_reader = new WKBReader(GeoUtils.GEOM_FACT);
		m_writer = new WKBWriter();
	}
	
	public RecordLite deserialize(Row row) {
		Object[] values = Rows.getValueList(row).toArray();
		deserialize(values);
		return RecordLite.of(values);
	}
	
	public void deserialize(Object[] values) {
		WKBReader reader = getWKBReader();
		
		for ( int i =0; i < m_colTypes.length; ++i  ) {
			JarveyDataType type = m_colTypes[i];
			if ( type.isGeometryType() ) {
				byte[] wkb = (byte[])values[i];
				try {
					values[i] = (wkb != null) ? reader.read(wkb) : null;
				}
				catch ( ParseException e ) {
					throw new DatasetOperationException(e);
				}
			}
			else {
				values[i] = type.deserialize(values[i]);
			}
		}
	}
	
	public Row serialize(RecordLite rec) {
		Object[] serialized = Arrays.copyOf(rec.values(), rec.length());
		serialize(serialized);
		return Rows.toRow(serialized);
	}
	
	public void serialize(Object[] values) {
		WKBWriter writer = getWKBWriter();
		
		for ( int i =0; i < m_colTypes.length; ++i  ) {
			JarveyDataType type = m_colTypes[i];
			if ( type.isGeometryType() ) {
				Geometry geom = (Geometry)values[i];
				if ( geom != null ) {
					values[i] = writer.write(geom);
				}
			}
			else {
				values[i] = type.serialize(values[i]);
			}
		}
	}
	
	private WKBReader getWKBReader() {
		return (m_reader != null) ? m_reader : new WKBReader(GeoUtils.GEOM_FACT);
	}
	
	private WKBWriter getWKBWriter() {
		return (m_writer != null) ? m_writer : new WKBWriter();
	}
}
