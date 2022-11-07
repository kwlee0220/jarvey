package jarvey.optor.geom;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Geometry;

import jarvey.support.RecordLite;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveySchema;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryToScalarFunction<T> extends GeometryFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_outCol;
	
	private int m_outColIdx;
	
	abstract protected JarveyDataType toScalarType(GeometryType geomType, JarveySchema inputSchema);
	abstract protected T toScalar(@Nullable Geometry geom, RecordLite inputRecord);
	
	protected GeometryToScalarFunction(String outCol) {
		Utilities.checkNotNullArgument(outCol, "output column");
		
		m_outCol = outCol;
	}

	@Override
	protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema) {
		JarveyDataType outType = toScalarType(geomType, inputSchema);
		JarveySchema outSchema = inputSchema.toBuilder()
											.addOrReplaceJarveyColumn(m_outCol, outType)
											.build();
		m_outColIdx = outSchema.getColumn(m_outCol).getIndex();
		
		return outSchema;
	}

	@Override
	protected RecordLite apply(Geometry geom, RecordLite inputRecord) {
		RecordLite output = RecordLite.of(getOutputSchema());
		inputRecord.copyTo(output);
		
		Object scalar = toScalar(geom, inputRecord);
		output.set(m_outColIdx, scalar);
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s", getClass().getSimpleName(), m_outCol);
	}
}
