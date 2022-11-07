package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.geo.util.GeometryUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToPoint extends GeometryFactory {
	private static final long serialVersionUID = 1L;
	
	private final String m_xCol;
	private final String m_yCol;
	private final int m_srid;
	
	private int m_xColIdx;
	private int m_yColIdx;
	
	public ToPoint(String xCol, String yCol, int srid, String ptCol) {
		super(ptCol);
		
		m_xCol = xCol;
		m_yCol = yCol;
		m_srid = srid;
	}

	@Override
	protected GeometryType toOutputGeometryType(JarveySchema inputSchema) {
		m_xColIdx = inputSchema.getColumnIndex(m_xCol);
		m_yColIdx = inputSchema.getColumnIndex(m_yCol);
		
		return JarveyDataTypes.PointType.newGeometryType(m_srid);
	}

	@Override
	protected Geometry toOutputGeometry(RecordLite input) {
		return GeometryUtils.toPoint(input.getDouble(m_xColIdx), input.getDouble(m_yColIdx));
	}
	
	@Override
	public String toString() {
		return String.format("%s: (%s, %s) -> %s", getClass().getSimpleName(),
							m_xCol, m_yCol, m_outGeomCol);
	}
}