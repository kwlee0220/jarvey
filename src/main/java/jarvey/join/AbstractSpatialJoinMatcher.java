package jarvey.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.type.JarveySchema;
import utils.LoggerSettable;
import utils.geo.util.CoordinateTransform;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractSpatialJoinMatcher implements SpatialJoinMatcher, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(AbstractSpatialJoinMatcher.class);
	
	protected JarveySchema m_left = null; 
	protected JarveySchema m_right = null;
	protected CoordinateTransform m_trans;
	private Logger m_logger = s_logger;

	@Override
	public void open(JarveySchema left, JarveySchema right) {
		m_left = left;
		m_right = right;
		
		int leftSrid = m_left.getDefaultGeometryColumn().getJarveyDataType().asGeometryType().getSrid();
		int rightSrid = m_right.getDefaultGeometryColumn().getJarveyDataType().asGeometryType().getSrid();
		
		if ( leftSrid != rightSrid ) {
			throw new IllegalArgumentException(String.format("incompatible srid: %d <-> %d",
																leftSrid, rightSrid));
		}
		m_trans = (leftSrid != 4326) ? CoordinateTransform.getTransformToWgs84("EPSG:"+leftSrid) : null;
	}
	@Override public void close() { }
	
	@Override
	public Logger getLogger() {
		return m_logger;
	}
	
	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public String toString() {
		return toStringExpr();
	}
}
