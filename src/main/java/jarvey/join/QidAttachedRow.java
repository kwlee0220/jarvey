package jarvey.join;

import java.util.Set;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Sets;

import jarvey.SpatialDataset;
import jarvey.quadtree.Enveloped;
import jarvey.type.JarveySchema;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class QidAttachedRow implements Enveloped {
	private static final String ENVL = SpatialDataset.ENVL4326;
	private static final String MEMBERS = SpatialDataset.CLUSTER_MEMBERS;
	private static final String ID = SpatialDataset.CLUSTER_ID;
	
	private final JarveySchema m_jschema;
	private final Row m_row;
	private Geometry m_geom;
	private final Envelope m_envl4326;
	private Set<Long> m_members;
	private Long m_clusterId;
	
	public QidAttachedRow(JarveySchema jschema, Row row) {
		m_jschema = jschema;
		m_row = row;
		
		m_envl4326 = (Envelope)m_jschema.getValue(row, ENVL);
	}
	
	public Row getRow() {
		return m_row;
	}

	@Override
	public Envelope getEnvelope84() {
		return m_envl4326;
	}
	
	public Set<Long> getClusterMembers() {
		if ( m_members == null ) {
			Long[] array = (Long[])m_jschema.getValue(m_row, MEMBERS);
			m_members = Sets.newHashSet(array);
		}
		
		return m_members;
	}
	
	public Long getClusterId() {
		if ( m_clusterId == null ) {
			m_clusterId = (Long)m_jschema.getValue(m_row, ID);
		}
		
		return m_clusterId;
	}
	
	public Geometry getGeometry() {
		if ( m_geom == null ) {
			m_geom = m_jschema.getDefaultGeometry(m_row);
		}
		
		return m_geom;
	}
	
	public <T> T getAs(String colName) {
		return m_row.getAs(colName);
	}
}
