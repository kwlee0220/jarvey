package jarvey.optor.geom;

import org.locationtech.jts.geom.Geometry;

import jarvey.JarveySession;
import jarvey.optor.RecordLevelFunction;
import jarvey.support.RecordLite;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryFactory extends RecordLevelFunction {
	private static final long serialVersionUID = 1L;
	
	protected final String m_outGeomCol;
	
	private JarveySchema m_outputSchema;
	private GeometryColumnInfo m_outputGcInfo;
	protected int m_outGeomColIdx = -1;
	
	abstract protected GeometryType toOutputGeometryType(JarveySchema inputSchema);
	abstract protected Geometry toOutputGeometry(RecordLite inputRecord);
	
	protected GeometryFactory(String outGeomCol) {
		Utilities.checkNotNullArgument(outGeomCol, "Output Geometry column");

		m_outGeomCol = outGeomCol;
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		GeometryType outputGeomType = toOutputGeometryType(inputSchema);
		m_outputSchema = inputSchema.toBuilder()
									.addOrReplaceJarveyColumn(m_outGeomCol, outputGeomType)
									.build();
		m_outGeomColIdx = m_outputSchema.getColumn(m_outGeomCol).getIndex();
		m_outputGcInfo = new GeometryColumnInfo(m_outGeomCol, outputGeomType);
		
		super.initialize(jarvey, inputSchema);
	}

	@Override
	public JarveySchema getOutputSchema() {
		return m_outputSchema;
	}
	
	public GeometryColumnInfo getOutputGeometryColumnInfo() {
		return m_outputGcInfo;
	}

	@Override
	protected RecordLite mapRecord(RecordLite input) {
		RecordLite output = RecordLite.of(getOutputSchema());
		input.copyTo(output);
		
		Geometry outGeom = toOutputGeometry(input);
		output.set(m_outGeomColIdx, outGeom);
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s: out=%s", getClass().getSimpleName(), m_outGeomCol);
	}
}
