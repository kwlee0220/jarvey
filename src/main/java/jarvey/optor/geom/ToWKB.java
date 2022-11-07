package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ToWKB extends GeometryToScalarFunction<byte[]> {
	private static final long serialVersionUID = 1L;
	private static final byte[] EMPTY_WKB = new byte[0];
	
	private transient WKBWriter m_writer;
	
	public ToWKB(String wkbCol) {
		super(wkbCol);
	}
	
	protected void initializeTask() {
		m_writer = new WKBWriter();
	}

	@Override
	protected JarveyDataType toScalarType(GeometryType geomType, JarveySchema inputSchema) {
		return JarveyDataTypes.Binary_Type;
	}

	@Override
	protected byte[] toScalar(Geometry geom, RecordLite inputRecord) {
		if ( geom != null ) {
			return (geom.isEmpty()) ? EMPTY_WKB : m_writer.write(geom);
		}
		else {
			return null;
		}
	}
}