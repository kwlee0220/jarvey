package jarvey.optor.geom;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;

import jarvey.datasource.DatasetOperationException;
import jarvey.support.GeoUtils;
import jarvey.support.RecordLite;

import utils.geo.util.GeometryUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangePredicate extends GeometryPredicate {
	private static final long serialVersionUID = 1L;
	
	private final SpatialRelation m_relation;
	private final byte[] m_rangeWkb;

	private transient PreparedGeometry m_range;
	
	public static RangePredicate intersects(Envelope range) {
		return new RangePredicate(GeometryUtils.toPolygon(range), SpatialRelation.INTERSECTS);
	}
	public static RangePredicate intersects(Geometry range) {
		return new RangePredicate(range, SpatialRelation.INTERSECTS);
	}
	public static RangePredicate isCoveredBy(Envelope range) {
		return new RangePredicate(GeometryUtils.toPolygon(range), SpatialRelation.COVERED_BY);
	}
	public static RangePredicate isCoveredBy(Geometry range) {
		return new RangePredicate(range, SpatialRelation.COVERED_BY);
	}
	
	private RangePredicate(Geometry range, SpatialRelation relation) {
		m_relation = relation;
		m_rangeWkb = GeoUtils.toWKB(range);
	}

	@Override
	protected void initializeTask() {
		try {
			Geometry geom = GeoUtils.fromWKB(m_rangeWkb);
			m_range = PreparedGeometryFactory.prepare(geom);
		}
		catch ( ParseException e ) {
			throw new DatasetOperationException(e);
		}
	}

	@Override
	protected boolean test(Geometry geom, RecordLite inputRecord) {
		return (geom != null) ? m_relation.test(geom, m_range) : false;
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_relation);
	}
}
