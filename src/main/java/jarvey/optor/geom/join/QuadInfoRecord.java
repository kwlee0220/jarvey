package jarvey.optor.geom.join;

import java.util.Collections;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Sets;

import jarvey.support.RecordLite;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadInfoRecord extends EnvelopedRecordLite {
	private final Long[] m_qids;
	private final long m_partId;
	
	public QuadInfoRecord(RecordLite rec, Geometry geom, Envelope envl4326, Long[] qids, long partId) {
		super(rec, geom, envl4326);
		
		m_qids = qids;
		m_partId = partId;
	}
	
	public Long[] getQuadIds() {
		return m_qids;
	}
	
	public long getPartitionId() {
		return m_partId;
	}
	
	public QuadInfoRecord duplicate() {
		return new QuadInfoRecord(getRecord().duplicate(), getGeometry(), getEnvelope84(), m_qids, m_partId);
	}
	
	public static boolean isNonReplica(QuadInfoRecord left, QuadInfoRecord right) {
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
