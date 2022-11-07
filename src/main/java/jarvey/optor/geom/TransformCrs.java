package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.geo.util.CoordinateTransform;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformCrs extends GeometryTransform {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(TransformCrs.class);
	
	private final int m_toSrid;
	
	private transient CoordinateTransform m_coordTrans;
	
	public TransformCrs(int toSrid, GeomOpOptions opts) {
		super(opts);
		Utilities.checkArgument(toSrid > 0, "invalid target srid");
		
		m_toSrid = toSrid;
		setLogger(s_logger);
	}
	
	protected void initializeTask() {
		int fromSrid = getInputGeometryColumnInfo().getSrid();
		m_coordTrans = CoordinateTransform.get(fromSrid, m_toSrid);
	}

	@Override
	protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema) {
		return GeometryType.of(geomType.getGeometries(), m_toSrid);
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		return m_coordTrans.transform(geom);
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s->%s", getClass().getSimpleName(),
								getInputGeometryColumnInfo().getSrid(),
								getOutputGeometryColumnInfo().getSrid());
	}
}
