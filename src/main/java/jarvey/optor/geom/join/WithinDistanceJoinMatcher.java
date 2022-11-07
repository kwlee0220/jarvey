package jarvey.optor.geom.join;

import static jarvey.optor.geom.SpatialRelation.WITHIN_DISTANCE;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.geom.SpatialRelation;
import jarvey.quadtree.Enveloped;
import jarvey.type.JarveySchema;

import utils.geo.util.CoordinateTransform;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class WithinDistanceJoinMatcher<T extends GeometryHolder & Enveloped> extends AbstractSpatialJoinMatcher<T> {
	private static final Logger s_logger = LoggerFactory.getLogger(WithinDistanceJoinMatcher.class);
	
	private double m_distance;
	private transient CoordinateTransform m_coordTrans = null;
	
	WithinDistanceJoinMatcher(double distance) {
		m_distance = distance;
		
		setLogger(s_logger);
	}

	@Override
	public void open(JarveySchema left, JarveySchema right) {
		super.open(left, right);
		
		// Left DataFrame과 right DataFrame은 동일 SRID를 갖기 때문에
		// left DataFrame의 srid만 체크하면 된다.
		if ( left.getSrid() == 4326 ) {
			throw new IllegalArgumentException("SRID(4326) is not supported");
		}
		
		m_coordTrans = CoordinateTransform.getTransformToWgs84("EPSG:" + left.getSrid());
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return WITHIN_DISTANCE(m_distance);
	}

	@Override
	public FStream<T> match(T left, SpatialLookupTable<T> slut) {
		Geometry outerGeom = left.getGeometry();
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.empty();
		}
		
		Envelope key = outerGeom.getEnvelopeInternal();
		key.expandBy(m_distance);
		key = m_coordTrans.transform(key);
		
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
