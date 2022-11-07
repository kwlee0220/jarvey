package jarvey.optor.geom;

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
public class Area extends GeometryToScalarFunction<Double> {
	private static final long serialVersionUID = 1L;
	
	public Area(String outCol) {
		super(outCol);
	}

	@Override
	protected JarveyDataType toScalarType(GeometryType geomType, JarveySchema inputSchema) {
		return JarveyDataTypes.Double_Type;
	}

	@Override
	protected Double toScalar(Geometry geom, RecordLite inputRecord) {
		return (geom != null) ? geom.getArea() : 0;
	}
}