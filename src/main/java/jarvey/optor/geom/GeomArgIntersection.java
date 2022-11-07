package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomArgIntersection extends GeomArgGeometryOperator {
	private static final long serialVersionUID = 1L;
	
	private transient SafeIntersection m_intersection = null;
	
	public GeomArgIntersection(Geometry paramGeom, GeomOpOptions opts) {
		super(paramGeom, opts);
		
		setLogger(LoggerFactory.getLogger(GeomArgIntersection.class));
	}

	@Override
	protected void initializeTask() {
		super.initializeTask();

		GeometryType dstType = getInputGeometryColumn().getJarveyDataType().asGeometryType();
		m_intersection = new SafeIntersection(dstType);
	}

	@Override
	protected Geometry transform(Geometry geom, Geometry paramGeom, RecordLite inputRecord) {
		return m_intersection.apply(geom, paramGeom);
	}

	@Override
	public String toString() {
		return String.format("GeomArgIntersection");
	}
}
