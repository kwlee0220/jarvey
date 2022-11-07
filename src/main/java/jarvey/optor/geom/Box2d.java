package jarvey.optor.geom;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Box2d extends GeometryToScalarFunction<Envelope> {
	private static final long serialVersionUID = 1L;
	
	public Box2d(String boxCol) {
		super(boxCol);
	}

	@Override
	protected JarveyDataType toScalarType(GeometryType geomType, JarveySchema inputSchema) {
		return JarveyDataTypes.Envelope_Type;
	}

	@Override
	protected Envelope toScalar(Geometry geom, RecordLite inputRecord) {
		return (geom != null) ? geom.getEnvelopeInternal() : null;
	}
}