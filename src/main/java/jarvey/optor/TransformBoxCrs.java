package jarvey.optor;

import org.locationtech.jts.geom.Envelope;

import jarvey.JarveySession;
import jarvey.support.RecordLite;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.geo.util.CoordinateTransform;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformBoxCrs extends RecordLevelFunction {
	private static final long serialVersionUID = 1L;
	
	private final String m_boxCol;
	private final String m_outCol;
	private final int m_fromSrid;
	private final int m_toSrid;
	
	private JarveySchema m_outputSchema;
	private int m_boxColIdx;
	private int m_outColIdx;
	
	private transient CoordinateTransform m_coordTrans;
	
	public TransformBoxCrs(String boxCol, String outCol, int fromSrid, int toSrid) {
		m_boxCol = boxCol;
		m_outCol = outCol;
		m_fromSrid = fromSrid;
		m_toSrid = toSrid;
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		m_boxColIdx = inputSchema.getColumn(m_boxCol).getIndex();
		
		m_outputSchema = inputSchema.toBuilder()
									.addOrReplaceJarveyColumn(m_outCol, JarveyDataTypes.Envelope_Type)
									.build();
		m_outColIdx = m_outputSchema.getColumn(m_outCol).getIndex();
		
		super.initialize(jarvey, inputSchema);
	}

	@Override
	public JarveySchema getOutputSchema() {
		return m_outputSchema;
	}
	
	@Override
	protected void initializeTask() {
		super.initializeTask();
		
		m_coordTrans = CoordinateTransform.get("EPSG:" + m_fromSrid, "EPSG:" + m_toSrid);
	}

	@Override
	protected RecordLite mapRecord(RecordLite input) {
		RecordLite output = RecordLite.of(m_outputSchema);
		input.copyTo(output);
		
		Envelope srcEnvl = input.getEnvelope(m_boxColIdx);
		Envelope transformed = (srcEnvl != null) ? m_coordTrans.transform(srcEnvl) : null;
		output.set(m_outColIdx, transformed);
		
		return output;
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s", getClass().getSimpleName(), m_boxCol);
	}
}