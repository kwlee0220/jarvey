package jarvey.optor.geom.join;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.geom.SpatialRelation;
import jarvey.quadtree.Enveloped;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntersectsJoinMatcher<T extends GeometryHolder & Enveloped> extends AbstractSpatialJoinMatcher<T> {
	private static final Logger s_logger = LoggerFactory.getLogger(IntersectsJoinMatcher.class);

	public IntersectsJoinMatcher() {
		setLogger(s_logger);
	}
	
	@Override
	public SpatialRelation toSpatialRelation() {
		return SpatialRelation.INTERSECTS;
	}

	@Override
	public FStream<T> match(T outer, SpatialLookupTable<T> slut) {
		Geometry outerGeom = outer.getGeometry();
		if ( outerGeom == null || outerGeom.isEmpty() ) {
			return FStream.empty();
		}
		
		PreparedGeometry pouter = PreparedGeometryFactory.prepare(outerGeom);
		return slut.query(outer.getEnvelope84(), true)
					.filter(inner ->  pouter.intersects(inner.getGeometry()));
	}
	
	@Override
	public String toStringExpr() {
		return "intersects";
	}
}
