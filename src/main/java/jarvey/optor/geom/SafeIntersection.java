package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import jarvey.support.GeoUtils;
import jarvey.support.JavaGeoms;
import jarvey.type.GeometryType;

import utils.geo.util.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SafeIntersection {
	private GeometryType m_resultType;
	private GeometryPrecisionReducer m_reducer = null;
	
	public SafeIntersection(GeometryType resultType) {
		m_resultType = resultType;
	}
	
	public SafeIntersection setReduceFactor(int factor) {
		m_reducer = (factor >= 0) ? GeoClientUtils.toGeometryPrecisionReducer(factor) : null;
		return this;
	}
	
	public Geometry apply(Geometry geom1, Geometry geom2) {
		Geometry result = null;
		try {
			result = geom1.intersection(geom2);
		}
		catch ( Exception e ) {
			try {
				result = JavaGeoms.intersection(geom1, geom2);
				if ( result == null ) {
					throw e;
				}
			}
			catch ( Exception ignored ) {
				result = intersectionWithPrecision(geom1, geom2);
			}
		}
		
		if ( m_resultType != null ) {
			result = GeoUtils.cast(result, m_resultType.getGeometries());
		}
		
		return result;
	}
	
	private Geometry intersectionWithPrecision(Geometry geom1, Geometry geom2) {
		if ( m_reducer == null ) {
			return m_resultType.newEmptyInstance();
		}
		
		geom1 = m_reducer.reduce(geom1);
		geom2 = m_reducer.reduce(geom2);
		
		return geom1.intersection(geom2);
	}
}
