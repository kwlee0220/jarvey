package jarvey.optor.geom.join;

import java.util.Collections;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Sets;

import jarvey.quadtree.Enveloped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadInfoRow implements Enveloped, GeometryHolder {
	private final Row m_row;
	private final Geometry m_geom;
	private final Envelope m_envl4326;
	private final Long[] m_qids;
	private final long m_partId;
	
	public QuadInfoRow(Row row, Geometry geom, Envelope envl4326, Long[] qids, long partId) {
		m_row = row;
		m_geom = geom;
		m_envl4326 = envl4326;
		m_qids = qids;
		m_partId = partId;
	}
	
	public Row getRow() {
		return m_row;
	}

	@Override
	public Geometry getGeometry() {
		return m_geom;
	}
	
	@Override
	public Envelope getEnvelope84() {
		return m_envl4326;
	}
	
	public Long[] getQuadIds() {
		return m_qids;
	}
	
	public long getPartitionId() {
		return m_partId;
	}
	
	public static boolean isNonReplica(QuadInfoRow left, QuadInfoRow right) {
		Long[] leftQids = left.getQuadIds();
		if ( leftQids.length == 1 ) {
			return true;
		}
		
		Long[] rightQids = right.getQuadIds();
		if ( rightQids.length == 1 ) {
			return true;
		}
		
		Set<Long> overlap = Sets.intersection(Sets.newHashSet(leftQids), Sets.newHashSet(rightQids));
		return left.getPartitionId() == Collections.max(overlap).longValue();
	}
}
