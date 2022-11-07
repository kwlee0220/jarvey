package jarvey.optor.geom;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRelation implements Serializable {
	private static final long serialVersionUID = 1L;

	public enum Code {
		CODE_ALL, CODE_INTERSECTS, CODE_COVERS, CODE_COVERED_BY, CODE_WITHIN_DISTANCE,
	}
	
	private final Code m_code;
	protected boolean m_negate;
	
	public abstract boolean test(Geometry leftGeom, Geometry rightGeom);
	public abstract boolean test(Geometry leftGeom, PreparedGeometry rightGeom);
	public abstract String toStringExpr();
	
	private SpatialRelation(Code code) {
		this(code, false);
	}
	
	private SpatialRelation(Code code, boolean negated) {
		m_code = code;
		m_negate = negated;
	}
	
	public Code getCode() {
		return m_code;
	}

	@Override
	public String toString() {
		return toStringExpr();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof SpatialRelation) ) {
			return false;
		}
		
		SpatialRelation other = (SpatialRelation)obj;
		return m_code == other.m_code;
	}
	
	public static SpatialRelation parse(String expr) {
		switch ( expr ) {
			case "all":
				return ALL;
			case "intersects":
				return INTERSECTS;
			case "not_intersects":
				return NOT_INTERSECTS;
			case "covers":
				return COVERS;
			case "not_covers":
				return NOT_COVERS;
			case "covered_by":
				return COVERED_BY;
			case "not_covered_by":
				return NOT_COVERED_BY;
			default:
				String[] parts = parseFuncExpr(expr);
				if ( "within_distance".startsWith(parts[0]) ) {
					double dist = Double.parseDouble(parts[1]);
					return new WithinDistanceRelation(dist, false);
				}
				else if ( "not_within_distance".startsWith(parts[0]) ) {
					double dist = Double.parseDouble(parts[1]);
					return new WithinDistanceRelation(dist, true);
				}
				else {
					throw new IllegalArgumentException("invalid SpatialRelation expression: expr=" + expr);
				}
		}
	}
	
	private static String[] parseFuncExpr(String expr) {
		int begin = expr.indexOf('(');
		if ( begin < 0 ) {
			return null;
		}
		int end = expr.indexOf(')', begin);
		if ( end < 0 ) {
			return null;
		}
		
		List<String> parsed = Lists.newArrayList(expr.substring(0, begin));
		parsed.addAll(Arrays.asList(expr.substring(begin+1, end).split(",")));
		return parsed.toArray(new String[0]);
	}
	
	public static final SpatialRelation ALL = new SpatialRelation(Code.CODE_ALL) {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return true;
		}

		@Override
		public boolean test(Geometry leftGeom, PreparedGeometry rightGeom) {
			return false;
		}
		
		@Override
		public String toStringExpr() {
			return "all";
		}
	};
	
	public static final class IntersectsRelation extends SpatialRelation {
		private static final long serialVersionUID = 1L;
		
		private IntersectsRelation(boolean negate) {
			super(Code.CODE_INTERSECTS, negate);
		}
		
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			boolean result = leftGeom.intersects(rightGeom);
			return m_negate ? !result : result;
		}
		
		public boolean test(Geometry leftGeom, PreparedGeometry rightGeom) {
			boolean result = rightGeom.intersects(leftGeom);
			return m_negate ? !result : result;
		}

		@Override
		public String toStringExpr() {
			return m_negate ? "not_intersects" : "intersects";
		}
	};
	public static final IntersectsRelation INTERSECTS = new IntersectsRelation(false);
	public static final IntersectsRelation NOT_INTERSECTS = new IntersectsRelation(true);

	public static final class CoversRelation extends SpatialRelation {
		private static final long serialVersionUID = 1L;
		
		private CoversRelation(boolean negate) {
			super(Code.CODE_COVERS, negate);
		}
		
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			boolean result = leftGeom.covers(rightGeom);
			return m_negate ? !result : result;
		}

		@Override
		public boolean test(Geometry leftGeom, PreparedGeometry rightGeom) {
			boolean result = rightGeom.coveredBy(leftGeom);
			return m_negate ? !result : result;
		}

		@Override
		public String toStringExpr() {
			return m_negate ? "not_covers" : "covers";
		}
	};
	public static final CoversRelation COVERS = new CoversRelation(false);
	public static final CoversRelation NOT_COVERS = new CoversRelation(true);


	private static final class CoveredByRelation extends SpatialRelation {
		private static final long serialVersionUID = 1L;
		
		private CoveredByRelation(boolean negate) {
			super(Code.CODE_COVERED_BY, negate);
		}
		
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			boolean result = leftGeom.coveredBy(rightGeom);
			return m_negate ? !result : result;
		}

		@Override
		public boolean test(Geometry leftGeom, PreparedGeometry rightGeom) {
			boolean result = rightGeom.covers(leftGeom);
			return m_negate ? !result : result;
		}

		@Override
		public String toStringExpr() {
			return m_negate ? "not_covered_by" : "covered_by";
		}
	};
	public static final CoveredByRelation COVERED_BY = new CoveredByRelation(false);
	public static final CoveredByRelation NOT_COVERED_BY = new CoveredByRelation(true);

	public static final class WithinDistanceRelation extends SpatialRelation {
		private static final long serialVersionUID = 1L;
		
		private final double m_distance;
		
		public WithinDistanceRelation(double distance, boolean negate) {
			super(Code.CODE_WITHIN_DISTANCE, negate);
			
			Preconditions.checkArgument(distance >= 0);
			m_distance = distance;
		}
		
		public double getDistance() {
			return m_distance;
		}

		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			if ( leftGeom == null || rightGeom == null
				|| leftGeom.isEmpty() || rightGeom.isEmpty() ) {
				return false;
			}
			
			return leftGeom.isWithinDistance(rightGeom, m_distance);
		}

		@Override
		public boolean test(Geometry leftGeom, PreparedGeometry rightGeom) {
			if ( leftGeom == null || rightGeom == null || leftGeom.isEmpty() ) {
				return false;
			}
			
			return test(leftGeom, rightGeom.getGeometry());
		}

		@Override
		public String toStringExpr() {
			String prefix = m_negate ? "not_" : "";
			return String.format("%swithin_distance(%.1f)", prefix, m_distance);
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( !super.equals(obj) ) {
				return false;
			}
			
			WithinDistanceRelation other = (WithinDistanceRelation)obj;
			return Double.compare(m_distance, other.m_distance) == 0;
		}
	}
	public static final WithinDistanceRelation WITHIN_DISTANCE(double distance) {
		return new WithinDistanceRelation(distance, false);
	}
	public static SpatialRelation WITHIN_DISTANCE(String distStr) {
		double dist = UnitUtils.parseLengthInMeter(distStr);
		return new WithinDistanceRelation(dist, false);
	}
	public static final WithinDistanceRelation NOT_WITHIN_DISTANCE(double distance) {
		return new WithinDistanceRelation(distance, true);
	}
	public static SpatialRelation NOT_WITHIN_DISTANCE(String distStr) {
		double dist = UnitUtils.parseLengthInMeter(distStr);
		return new WithinDistanceRelation(dist, true);
	}
}
