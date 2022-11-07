package jarvey.optor.geom.join;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.SpatialDataFrame;
import jarvey.optor.RecordSerDe;
import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadInfoRecordSerDe {
	private final RecordSerDe m_serde;
	private final int m_geomColIdx;
	private final int m_envlColIdx;
	private final int m_qidsColIdx;
	private final int m_partColIdx;

	public static QuadInfoRecordSerDe of(JarveySchema schema) {
		return new QuadInfoRecordSerDe(schema);
	}
	
	private QuadInfoRecordSerDe(JarveySchema jschema) {
		m_serde = RecordSerDe.of(jschema);
		m_geomColIdx = jschema.assertDefaultGeometryColumn().getIndex();
		m_envlColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_ENVL4326).getIndex();
		m_qidsColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_QUAD_IDS).getIndex();
		m_partColIdx = jschema.getColumn(SpatialDataFrame.COLUMN_PARTITION_QID).getIndex();
	}
	
	public QuadInfoRecord deserialize(Row row) {
		RecordLite rec = m_serde.deserialize(row);
		Geometry geom = rec.getGeometry(m_geomColIdx);
		Envelope envl4326 = rec.getEnvelope(m_envlColIdx);
		Long[] quids = rec.getLongArray(m_qidsColIdx);
		long partId = rec.getLong(m_partColIdx);
		
		return new QuadInfoRecord(rec, geom, envl4326, quids, partId);
	}
	
	public Row serialize(QuadInfoRecord rec) {
		return m_serde.serialize(rec.getRecord());
	}
}
