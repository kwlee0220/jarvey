package jarvey.optor;

import java.util.Iterator;

import jarvey.JarveySession;
import jarvey.support.RecordLite;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignUid extends AbstractRDDFunction {
	private static final long serialVersionUID = 1L;
	private static final long MAX_RECS_PER_SPLIT = 1_000_000_000L;
	
	private final String m_uidColumn;
	
	private JarveySchema m_outputSchema;
	private int m_uidColIdx;
	private long m_startId = -1;	// just for 'toString()'
	private long m_idGen = 0;
	
	public AssignUid(String colName) {
		super(true);
		Utilities.checkNotNullArgument(colName, "id column");
		
		m_uidColumn = colName;
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		m_outputSchema = inputSchema.toBuilder()
									.addOrReplaceJarveyColumn(m_uidColumn, JarveyDataTypes.Long_Type)
									.build();
		m_uidColIdx = m_outputSchema.getColumn(m_uidColumn).getIndex();
		
		super.initialize(jarvey, inputSchema);
	}

	@Override
	public JarveySchema getOutputSchema() {
		return m_outputSchema;
	}

	@Override
	protected Iterator<RecordLite> mapPartition(int partIdx, Iterator<RecordLite> iter) {
		checkInitialized();

		m_idGen = (long)MAX_RECS_PER_SPLIT * partIdx;
		return FStream.from(iter)
						.map(this::transform)
						.iterator();
	}
	
	@Override
	public String toString() {
		return String.format("%s: col=%s, start=%d, next=%d", getClass().getSimpleName(),
							m_uidColumn, m_startId, m_idGen);
	}

	private RecordLite transform(RecordLite input) {
		RecordLite output = RecordLite.of(getOutputSchema());
		
		output.set(input);
		output.set(m_uidColIdx, m_idGen++);
		return output;
	}
}