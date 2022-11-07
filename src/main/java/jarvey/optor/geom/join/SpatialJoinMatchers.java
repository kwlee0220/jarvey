package jarvey.optor.geom.join;

import jarvey.optor.geom.SpatialRelation;
import jarvey.optor.geom.SpatialRelation.WithinDistanceRelation;
import jarvey.quadtree.Enveloped;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinMatchers {
	private SpatialJoinMatchers() {
		throw new AssertionError();
	}
	
	public static <T extends GeometryHolder & Enveloped> SpatialJoinMatcher<T> WITHIN_DISTANCE(double distance) {
		return new WithinDistanceJoinMatcher<>(distance);
	}
	
	public static <T extends GeometryHolder & Enveloped> SpatialJoinMatcher<T> parse(String expr) {
		return from(SpatialRelation.parse(expr));
	}
	
	public static <T extends GeometryHolder & Enveloped> SpatialJoinMatcher<T> from(SpatialRelation relation) {
		if ( SpatialRelation.INTERSECTS.equals(relation) ) {
			return new IntersectsJoinMatcher<>();
		}
		else if ( relation instanceof WithinDistanceRelation ) {
			double distance = ((WithinDistanceRelation)relation).getDistance();
			return WITHIN_DISTANCE(distance);
		}
		else {
			throw new IllegalArgumentException("unsupported SpatialRelation: " + relation);
		}
	}
}
