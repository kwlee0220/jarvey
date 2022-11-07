package jarvey.optor;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;
import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

import utils.LoggerSettable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRDDFunction implements RDDFunction, Serializable, LoggerSettable {
	private static final long serialVersionUID = 1L;
	
	private final boolean m_preservePartitions;

	private JarveySession m_jarvey;
	private JarveySchema m_inputSchema;
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	private transient int m_partitionIndex = -1;
	
	protected abstract Iterator<RecordLite> mapPartition(int partitionIndex, Iterator<RecordLite> iter);
	
	protected AbstractRDDFunction(boolean preservePartitions) {
		m_preservePartitions = preservePartitions;
	}

	@Override
	public void initialize(JarveySession jarvey, JarveySchema inputSchema) {
		m_jarvey = jarvey;
		m_inputSchema = inputSchema;
	}
	
	public boolean isInitialized() {
		return m_jarvey != null;
	}
	
	public void checkInitialized() {
		if ( m_jarvey == null ) {
			throw new IllegalStateException("not initialized");
		}
	}
	
	public int getPartitionIndex() {
		checkInitialized();
		
		return m_partitionIndex;
	}
	
	protected void initializeTask() { }
	
	/**
	 * 데이터프레임 함수 초기화시 설정된 입력 데이터프레임 스키마를 반환한다.
	 * 
	 * @return	입력 데이터프레임 스키마
	 */
	public final JarveySchema getInputSchema() {
		return m_inputSchema;
	}
	
	@Override
	public JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input) {
		checkInitialized();
		
		return input.mapPartitionsWithIndex((pidx, recs) -> mapPartition(pidx, recs), m_preservePartitions);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
}
