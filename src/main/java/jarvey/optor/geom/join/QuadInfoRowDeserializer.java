package jarvey.optor.geom.join;

import javax.annotation.Nullable;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import jarvey.SpatialDataFrame;
import jarvey.datasource.DatasetOperationException;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadInfoRowDeserializer {
	private final int m_geomColIdx;
	private final int m_envlColIdx;
	private final int m_qidsColIdx;
	private final int m_partColIdx;
	private @Nullable transient WKBReader m_reader = null;

	public static QuadInfoRowDeserializer of(JarveySchema schema) {
		return new QuadInfoRowDeserializer(schema);
	}
	
	private QuadInfoRowDeserializer(JarveySchema jschema) {
		m_geomColIdx = jschema.assertDefaultGeometryColumn().getIndex();
		m_envlColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_ENVL4326).getIndex();
		m_qidsColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_QUAD_IDS).getIndex();
		m_partColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_PARTITION_QID).getIndex();
		m_reader = new WKBReader();
	}
	
	public QuadInfoRow deserialize(Row row) {
		try {
			Geometry geom = m_reader.read((byte[])row.get(m_geomColIdx));
			Envelope envl4326 = JarveyDataTypes.Envelope_Type.deserialize(row.get(m_envlColIdx));
			Long[] qids = JarveyDataTypes.LongArray_Type.deserialize(row.get(m_qidsColIdx));
			long partQid = row.getLong(m_partColIdx);
			
			return new QuadInfoRow(row, geom, envl4326, qids, partQid);
		}
		catch ( ParseException e ) {
			throw new DatasetOperationException(e);
		}
	}
}
