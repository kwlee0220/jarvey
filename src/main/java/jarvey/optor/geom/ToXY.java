package jarvey.optor.geom;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToXY extends GeometryFunction {
	private static final Logger s_logger = LoggerFactory.getLogger(ToXY.class);
	private static final long serialVersionUID = 1L;
	
	private final String m_xCol;
	private final String m_yCol;
	
	private int m_xColIdx;
	private int m_yColIdx;
	
	public ToXY(String xCol, String yCol) {
		super();
		Utilities.checkNotNullArgument(xCol, "x-pos column");
		Utilities.checkNotNullArgument(xCol, "y-pos column");
		
		m_xCol = xCol;
		m_yCol = yCol;
		
		setLogger(s_logger);
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		JarveySchema outSchema = inputSchema.toBuilder()
											.addOrReplaceJarveyColumn(m_xCol, JarveyDataTypes.Double_Type)
											.addOrReplaceJarveyColumn(m_yCol, JarveyDataTypes.Double_Type)
											.build();
		m_xColIdx = outSchema.getColumn(m_xCol).getIndex();
		m_yColIdx = outSchema.getColumn(m_yCol).getIndex();
		
		return outSchema;
	}

	@Override
	protected RecordLite apply(Geometry geom, RecordLite input) {
		RecordLite output = RecordLite.of(getOutputSchema());
		output.set(input);
		
		if ( geom != null && !geom.isEmpty() ) {
			Coordinate coord = geom.getCoordinate();
			output.set(m_xColIdx, coord.getX());
			output.set(m_yColIdx, coord.getY());
		}
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s,%s)", getClass().getSimpleName(), m_xCol, m_yCol);
	}
}