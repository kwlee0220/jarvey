package jarvey.datasource;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.StopWatch;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MultiSourcePartitionReader<T,R extends PartitionReader<InternalRow>>
	extends ChainedPartitionReader<R> {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiSourcePartitionReader.class);
	
	private final FStream<T> m_sources;
	private FOption<T> m_current = null;
	private StopWatch m_watch = StopWatch.create();
	
	abstract protected R getPartitionReader(T source) throws IOException;
	
	protected MultiSourcePartitionReader(FStream<T> sources) {
		Utilities.checkNotNullArgument(sources, "sources is null");
		
		m_sources = sources;
		setLogger(s_logger);
	}
	
	@Override
	protected void closeInGuard() throws IOException {
		try {
			m_sources.close();
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			Throwables.throwIfInstanceOf(cause, IOException.class);
			Throwables.toRuntimeException(cause);
		}
		finally {
			super.closeInGuard();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s", getClass().getSimpleName());
	}

	@Override
	protected R getNextPartitionReader() throws IOException {
		if ( m_current != null ) {
			getLogger().info("loaded: '{}', elapsed={}", m_current.get(), m_watch.stopAndGetElpasedTimeString());
		}
		
		m_current = m_sources.next();
		if ( m_current.isPresent() ) {	// we have another source to read
			m_watch.restart();
			
			return getPartitionReader(m_current.get());
		}
		else {
			return null;
		}
	}
}
