package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToWKT extends GeometryToScalarFunction<String> {
	private static final long serialVersionUID = 1L;
	private static final String EMPTY_WKT = "";
	
	private transient WKTWriter m_writer;
	
	public ToWKT(String wktCol) {
		super(wktCol);
	}
	
	protected void initializeTask() {
		m_writer = new WKTWriter();
	}

	@Override
	protected JarveyDataType toScalarType(GeometryType geomType, JarveySchema inputSchema) {
		return JarveyDataTypes.String_Type;
	}

	@Override
	protected String toScalar(Geometry geom, RecordLite inputRecord) {
		if ( geom != null ) {
			return (geom.isEmpty()) ? EMPTY_WKT : m_writer.write(geom);
		}
		else {
			return null;
		}
	}
}