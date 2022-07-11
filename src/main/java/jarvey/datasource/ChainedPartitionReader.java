package jarvey.datasource;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ChainedPartitionReader extends AbstractPartitionReader {
	@Nullable private PartitionReader<InternalRow> m_current = null;	// null이면 first-call 의미
	private boolean m_eos = false;
	
	/**
	 * 다음으로 접근할 {@code PartitionReader} 객체를 반환한다.
	 * <p>
	 * 만일 더 이상의 {@code PartitionReader}가 없는 경우는 {@code null}을 반환한다.
	 * 
	 * @return PartitionReader 또는 {@code null}.
	 */
	abstract protected PartitionReader<InternalRow> getNextPartitionReader() throws IOException;

	@Override
	protected void closeInGuard() throws IOException {
		IOUtils.closeQuietly(m_current);
		
		super.close();
	}

	@Override
	public boolean next() throws IOException {
		checkNotClosed();
		
		if ( m_eos ) {
			return false;
		}
		
		// first-call?
		if ( m_current == null ) { 
			if ( (m_current = getNextPartitionReader()) == null ) {
				m_eos = true;
				return false;
			}
		}
		
		while ( m_current.next() == false ) {
			m_current.close();

			if ( (m_current = getNextPartitionReader()) == null ) {
				m_eos = true;
				return false;
			}
		}
		
		return true;
	}

	@Override
	public InternalRow get() {
		if ( m_current == null ) {
			throw new IllegalStateException("the first next() has not been called");
		}
		
		return m_current.get();
	}
}