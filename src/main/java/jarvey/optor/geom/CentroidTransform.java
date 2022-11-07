package jarvey.optor.geom;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CentroidTransform extends GeometryTransform {
	private static final long serialVersionUID = 1L;
	
	private final boolean m_inside;
	
	public CentroidTransform(boolean inside, GeomOpOptions opts) {
		super(opts);
		
		m_inside = inside;
		setLogger(LoggerFactory.getLogger(CentroidTransform.class));
	}

	@Override
	protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema) {
		return GeometryType.of(Geometries.POINT, geomType.getSrid());
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		if ( geom == null ) {
			return null;
		}
		else if ( m_inside ) {
			return geom.getInteriorPoint();
		}
		else {
			return geom.getCentroid();
		}
	}
	
	@Override
	public String toString() {
		return String.format("Centroid[inside=%s]", m_inside);
	}
}
