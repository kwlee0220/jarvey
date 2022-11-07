package jarvey.optor.geom.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.quadtree.Enveloped;
import jarvey.type.JarveySchema;

import utils.LoggerSettable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractSpatialJoinMatcher<T extends GeometryHolder & Enveloped>
	implements SpatialJoinMatcher<T>, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(AbstractSpatialJoinMatcher.class);
	
	protected JarveySchema m_left;
	protected JarveySchema m_right;
	private Logger m_logger = s_logger;

	@Override
	public void open(JarveySchema left, JarveySchema right) {
		m_left = left;
		m_right = right;
		
		if ( left.getSrid() != right.getSrid() ) {
			throw new IllegalArgumentException(String.format("incompatible srid: %d <-> %d",
																left.getSrid(), right.getSrid()));
		}
//		m_trans = (leftSrid != 4326) ? CoordinateTransform.getTransformToWgs84("EPSG:"+leftSrid) : null;
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
