package jarvey.join;

import static jarvey.join.SpatialRelation.WITHIN_DISTANCE;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.type.JarveySchema;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class WithinDistanceJoinMatcher extends AbstractSpatialJoinMatcher {
	private static final Logger s_logger = LoggerFactory.getLogger(WithinDistanceJoinMatcher.class);
	
	private double m_distance;
	
	WithinDistanceJoinMatcher(double distance) {
		m_distance = distance;
		
		setLogger(s_logger);
	}

	@Override
	public void open(JarveySchema left, JarveySchema right) {
		super.open(left, right);
		
		if ( left.getDefaultGeometryColumn().getJarveyDataType().asGeometryType().getSrid() == 4326 ) {
			throw new IllegalArgumentException("SRID(4326) is not supported");
		}
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return WITHIN_DISTANCE(m_distance);
	}

	@Override
	public FStream<QidAttachedRow> match(QidAttachedRow outer, SpatialLookupTable<QidAttachedRow> slut) {
		Geometry outerGeom = outer.getGeometry();
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.empty();
		}
		
		Envelope key = outerGeom.getEnvelopeInternal();
		key.expandBy(m_distance);
		key = m_trans.transform(key);
		
		return slut.query(key, true)
					.filter(inner -> {
						Geometry innerGeom = inner.getGeometry();
						return outerGeom.isWithinDistance(innerGeom, m_distance);
					});
	}
	
	@Override
	public String toStringExpr() {
		return "within_distance(" + m_distance + ")";
	}
}
