package jarvey.datasource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.LoggerSettable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractPartitionReader implements PartitionReader<InternalRow>, LoggerSettable {
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	private AtomicBoolean m_closed = new AtomicBoolean(false);
	
	protected abstract void closeInGuard() throws IOException;
	
	public final boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public final void close() throws IOException {
		if ( m_closed.compareAndSet(false, true) ) {
			closeInGuard();
		}
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	protected final void checkNotClosed() throws IOException {
		if ( isClosed() ) {
			throw new IOException("already closed: this=" + getClass());
		}
	}
}
