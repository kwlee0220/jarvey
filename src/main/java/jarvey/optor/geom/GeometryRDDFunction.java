package jarvey.optor.geom;

import jarvey.JarveySession;
import jarvey.optor.AbstractRDDFunction;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryRDDFunction extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;

	private GeometryColumnInfo m_inputGcInfo;
	private JarveyColumn m_inputGeomCol;
	private int m_inputGeomColIdx;
	
	private JarveySchema m_outputSchema;

	abstract protected JarveySchema initialize(GeometryType geomType, JarveySchema inputSchema);
	
	protected GeometryRDDFunction() {
		super(true);
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		super.initialize(jarvey, inputSchema);
		
		m_inputGcInfo = inputSchema.assertDefaultGeometryColumnInfo();
		m_inputGeomCol = inputSchema.assertDefaultGeometryColumn();
		
		m_outputSchema = initialize(m_inputGeomCol.getJarveyDataType().asGeometryType(), inputSchema);
	}

	@Override
	public JarveySchema getOutputSchema() {
		checkInitialized();
		
		return m_outputSchema;
	}
	
	public GeometryColumnInfo getInputGeometryColumnInfo() {
		checkInitialized();
		
		return m_inputGcInfo;
	}
	
	public JarveyColumn getInputGeometryColumn() {
		checkInitialized();
		
		return m_inputGeomCol;
	}
	
	public int getInputGeometryColumnIndex() {
		return m_inputGeomColIdx;
	}
}
