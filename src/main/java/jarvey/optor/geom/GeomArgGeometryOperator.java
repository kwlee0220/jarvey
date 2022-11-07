package jarvey.optor.geom;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import jarvey.datasource.DatasetOperationException;
import jarvey.support.GeoUtils;
import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeomArgGeometryOperator extends GeometryTransform {
	private static final long serialVersionUID = 1L;
	
	private final byte[] m_paramWkb;
	
	private transient Geometry m_paramGeom;
	
	abstract protected @Nullable Geometry transform(Geometry geom, Geometry paramGeom, RecordLite inputRecord);
	
	public GeomArgGeometryOperator(Geometry paramGeom, GeomOpOptions opts) {
		super(opts);
		
		m_paramWkb = GeoUtils.toWKB(paramGeom);
	}

	@Override
	protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema) {
		return geomType;
	}
	
	@Override
	protected void initializeTask() { 
		try {
			m_paramGeom = GeoUtils.fromWKB(m_paramWkb);
		}
		catch ( ParseException e ) {
			throw new DatasetOperationException(e);
		}
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		return transform(geom, m_paramGeom, inputRecord);
	}
}
