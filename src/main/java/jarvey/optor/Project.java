package jarvey.optor;

import java.util.Set;

import com.google.common.base.Preconditions;

import jarvey.JarveySession;
import jarvey.support.RecordLite;
import jarvey.support.colexpr.ColumnSelector;
import jarvey.support.colexpr.SelectedColumnInfo;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveySchema;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Project extends RecordLevelFunction {
	private static final long serialVersionUID = 1L;

	private final String m_columnSelection;
	
	private int[] m_columnMapping;
	private JarveySchema m_outputSchema;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public Project(String columnSelection) {
		Preconditions.checkArgument(columnSelection != null, "Column seelection expression is null");
		
		m_columnSelection = columnSelection;
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		ColumnSelector selector = ColumnSelector.from(inputSchema, m_columnSelection);
		Set<SelectedColumnInfo> selection = selector.select();
		
		String[] cols = FStream.from(selection)
								.map(SelectedColumnInfo::getColumnName)
								.toArray(String.class);
		JarveySchema outSchema = inputSchema.select(cols);
		
		m_columnMapping = FStream.of(cols)
									.map(inputSchema::getColumn)
									.mapToInt(JarveyColumn::getIndex)
									.toArray();
		
		m_outputSchema = FStream.from(selection)
							.filter(info -> info.getAlias() != null)
							.foldLeft(outSchema, (acc,info) -> acc.rename(info.getColumnName(), info.getAlias()));
		
		super.initialize(jarvey, inputSchema);
	}

	@Override
	public JarveySchema getOutputSchema() {
		return m_outputSchema;
	}

	@Override
	protected RecordLite mapRecord(RecordLite input) {
		return input.select(m_columnMapping);
	}
	
	@Override
	public String toString() {
		return String.format("%s: '%s'", getClass().getSimpleName(), m_columnSelection);
	}
}