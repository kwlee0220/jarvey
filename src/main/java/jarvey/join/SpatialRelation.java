package jarvey.join;

import java.util.Arrays;
import java.util.List;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRelation {
	public enum Code {
		CODE_ALL, CODE_INTERSECTS, CODE_CONTAINS, CODE_IS_CONTAINED_BY, CODE_WITHIN_DISTANCE,
	}
	
	private final Code m_code;
	
	public abstract boolean test(Geometry leftGeom, Geometry rightGeom);
	public abstract String toStringExpr();
	
	public static SpatialRelation WITHIN_DISTANCE(double distance) {
		return new WithinDistanceRelation(distance);
	}
	
	public static SpatialRelation WITHIN_DISTANCE(String distStr) {
		double dist = UnitUtils.parseLengthInMeter(distStr);
		return new WithinDistanceRelation(dist);
	}
	
	private SpatialRelation(Code code) {
		m_code = code;
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
			case "contains":
				return CONTAINS;
			case "is_contained_by":
				return IS_CONTAINED_BY;
			default:
				String[] parts = parseFuncExpr(expr);
				if ( "within_distance".startsWith(parts[0]) ) {
					double dist = Double.parseDouble(parts[1]);
					return new WithinDistanceRelation(dist);
				}
				else {
					throw new IllegalArgumentException("invalid SpatialRelation expression: expr="
														+ expr);
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
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return true;
		}
		
		@Override
		public String toStringExpr() {
			return "all";
		}
	};
	public static final SpatialRelation INTERSECTS = new SpatialRelation(Code.CODE_INTERSECTS) {
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return leftGeom.intersects(rightGeom);
		}

		@Override
		public String toStringExpr() {
			return "intersects";
		}
	};
	public static final SpatialRelation CONTAINS = new SpatialRelation(Code.CODE_CONTAINS) {
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return leftGeom.contains(rightGeom);
		}

		@Override
		public String toStringExpr() {
			return "contains";
		}
	};
	public static final SpatialRelation IS_CONTAINED_BY = new SpatialRelation(Code.CODE_IS_CONTAINED_BY) {
		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return leftGeom.coveredBy(rightGeom);
		}

		@Override
		public String toStringExpr() {
			return "is_contained_by";
		}
	};
	
	public static class WithinDistanceRelation extends SpatialRelation {
		private final double m_distance;
		
		public WithinDistanceRelation(double distance) {
			super(Code.CODE_WITHIN_DISTANCE);
			
			Preconditions.checkArgument(distance >= 0);
			m_distance = distance;
		}
		
		public double getDistance() {
			return m_distance;
		}

		@Override
		public boolean test(Geometry leftGeom, Geometry rightGeom) {
			return leftGeom.isWithinDistance(rightGeom, m_distance);
		}

		@Override
		public String toStringExpr() {
			return String.format("within_distance(%.1f)", m_distance);
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
}
