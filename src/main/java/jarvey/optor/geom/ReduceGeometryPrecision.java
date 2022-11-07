package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.geo.util.GeoClientUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ReduceGeometryPrecision extends GeometryTransform {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BufferTransform.class);

	private int m_precisionFactor;
	
	private transient GeometryPrecisionReducer m_reducer = null;
	
	public ReduceGeometryPrecision(int precisionFactor, GeomOpOptions opts) {
		super(opts);

		m_precisionFactor = precisionFactor;
		setLogger(s_logger);
	}
	
	protected void initializeTask() {
		m_reducer = GeoClientUtils.toGeometryPrecisionReducer(m_precisionFactor);
	}

	@Override
	protected GeometryType toOutputGeometryType(GeometryType geomType, JarveySchema inputSchema) {
		return geomType;
	}

	@Override
	protected Geometry transform(Geometry geom, RecordLite inputRecord) {
		return m_reducer.reduce(geom);
	}
	
	@Override
	public String toString() {
		return String.format("%s[precision=%d]", getClass().getSimpleName(), m_precisionFactor);
	}
}
