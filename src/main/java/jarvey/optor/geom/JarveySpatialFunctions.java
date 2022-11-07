package jarvey.optor.geom;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.datasource.DatasetOperationException;
import jarvey.support.GeoUtils;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySpatialFunctions {
	private JarveySpatialFunctions() {
		throw new AssertionError("Should not be called: constructor = " + getClass().getName());
	}
	
	public static GeometryPredicate IsValidWgs84Geometry() {
		return new IsValidWgs84Geometry();
	}
	
	public static GeometryPredicate withinDistance(Geometry geom, double distance) {
		return new MatchSpatialRelation(geom, SpatialRelation.WITHIN_DISTANCE(distance));
	}
	
	public static RangePredicate intersectRange(Envelope range) {
		return RangePredicate.intersects(range);
	}
	public static RangePredicate intersectRange(Geometry range) {
		return RangePredicate.intersects(range);
	}
	
	private static class MatchSpatialRelation extends GeometryPredicate {
		private static final Logger s_logger = LoggerFactory.getLogger(MatchSpatialRelation.class);
		private static final long serialVersionUID = 1L;
		
		private transient Geometry m_geom;
		private byte[] m_wkb;
		private SpatialRelation m_rel;
		
		MatchSpatialRelation(Geometry geom, SpatialRelation rel) {
			Utilities.checkNotNullArgument(geom);
			Utilities.checkArgument(!geom.isEmpty(), "Geometry is empty");
			
			m_geom = geom;
			m_wkb = GeoUtils.toWKB(geom);
			m_rel = rel;
		}
		
		@Override
		protected void initializeTask() {
			super.initializeTask();
			
			try {
				m_geom = GeoUtils.fromWKB(m_wkb);
			}
			catch ( ParseException e ) {
				throw new DatasetOperationException(e);
			}
		}
		
		@Override
		protected boolean test(Geometry geom, RecordLite inputRecord) {
			try {
				return m_rel.test(geom, m_geom);
			}
			catch ( Exception e ) {
				s_logger.warn("fails to test: cause=" + e);
				return false;
			}
		}
		
		@Override
		public String toString() {
			return String.format("%s[%s: predicate=%s]",
								getClass().getSimpleName(),
								getInputGeometryColumnInfo().getName(), m_rel.toString());
		}
	}

	private static class IsValidWgs84Geometry extends GeometryPredicate {
		private static final long serialVersionUID = 1L;
	
		@Override
		protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
			if ( geomType.getSrid() != 4326 ) {
				throw new IllegalArgumentException("Input Geometry does not have WGS84 coordinates.");
			}
			
			return super.initialize(geomType, inputSchema);
		}
	
		@Override
		protected boolean test(Geometry geom, RecordLite inputRecord) {
			if ( geom == null || geom.isEmpty() ) {
				return false;
			}
			
			Coordinate c = geom.getCoordinate();
			if ( c.y > 88 ) {
				System.out.println(c);
			}
			
			return !FStream.of(geom.getCoordinates())
							.exists(coord -> (coord.x < -180 || coord.x > 180 || coord.y < -85 || coord.y > 85));
		}
		
		@Override
		public String toString() {
			return String.format("%s", getClass().getSimpleName());
		}
	}
}
