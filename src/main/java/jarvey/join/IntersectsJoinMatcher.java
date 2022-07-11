package jarvey.join;

import java.util.Collections;
import java.util.Set;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntersectsJoinMatcher extends AbstractSpatialJoinMatcher {
	private static final Logger s_logger = LoggerFactory.getLogger(IntersectsJoinMatcher.class);

	public IntersectsJoinMatcher() {
		setLogger(s_logger);
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return SpatialRelation.INTERSECTS;
	}

	@Override
	public FStream<QidAttachedRow> match(QidAttachedRow outer, SpatialLookupTable<QidAttachedRow> slut) {
		Geometry outerGeom = outer.getGeometry();
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.empty();
		}
		
		PreparedGeometry pouter = PreparedGeometryFactory.prepare(outerGeom);
		return slut.query(outer.getEnvelope84(), true)
					.filter(inner ->  pouter.intersects(inner.getGeometry()))
					.filter(inner -> isNonReplica(outer, inner));
	}
	
	@Override
	public String toStringExpr() {
		return "intersects";
	}
	
	private boolean isNonReplica(QidAttachedRow left, QidAttachedRow right) {
		Set<Long> rightMembers = right.getClusterMembers();
		if ( rightMembers.size() == 1 ) {
			return true;
		}
		
		Set<Long> leftMembers = left.getClusterMembers();
		leftMembers.retainAll(rightMembers);
		return right.getClusterId().equals(Collections.max(leftMembers));
	}
}
