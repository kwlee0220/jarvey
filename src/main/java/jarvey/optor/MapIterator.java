package jarvey.optor;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.LoggerSettable;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MapIterator<IN,OUT> implements Iterator<OUT>, LoggerSettable {
	private final Iterator<IN> m_iter;
	private OUT m_next;
	private Logger m_logger;
	
	private StopWatch m_watch;
	private long m_reportIntervalMills;
	private long m_lastReportedMillis;
	private int m_inCount = 0;
	private int m_outCount = 0;
	
	protected abstract OUT apply(IN input);
	protected String getHandlerString() { return ""; }
	
	public MapIterator(Iterator<IN> iter, long reportIntervalMillis) {
		m_watch = StopWatch.start();
		m_lastReportedMillis = m_watch.getElapsedInMillis();
		m_reportIntervalMills = reportIntervalMillis;
		m_logger = LoggerFactory.getLogger(getClass());

		m_iter = iter;
	}
	
	public MapIterator(Iterator<IN> iter) {
		this(iter, UnitUtils.parseDuration("1m"));
	}
	
	public int getInputCount() {
		return m_inCount;
	}
	
	public int getOutputCount() {
		return m_outCount;
	}

	@Override
	public boolean hasNext() {
		return m_next != null;
	}
	
	public void start() {
		m_next = lookAhead();
		if ( m_next == null ) {
			handleEndOfIterator();
		}
	}

	@Override
	public OUT next() {
		if ( m_next == null ) {
			throw new NoSuchElementException();
		}
		OUT next = m_next;
		
		m_next = lookAhead();
		if ( m_next == null ) {
			handleEndOfIterator();
		}
		
		return next;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	protected void onIteratorFinished() { }
	
	private OUT lookAhead() {
		while ( m_iter.hasNext() ) {
			long now = m_watch.getElapsedInMillis();
			if ( m_reportIntervalMills > 0
				&& (now - m_lastReportedMillis) >= m_reportIntervalMills ) {
				printProgress("progress");
				m_lastReportedMillis = now;
			}
			
			IN input = m_iter.next();
			++m_inCount;
			
			OUT output = apply(input);
			if ( output != null ) {
				++m_outCount;
				return output;
			}
		}
		
		return null;
	}
	
	private void handleEndOfIterator() {
		Unchecked.runOrIgnore(this::onIteratorFinished);
		printProgress("    done");
	}
	
	private void printProgress(String tag) {
		if ( getLogger().isInfoEnabled() ) {
			int partIdx = TaskContext.getPartitionId();
			double velo = m_inCount / m_watch.getElapsedInFloatingSeconds();
			double selectivity = (double)m_outCount / m_inCount;
			String msg = String.format("[%4d] %s: [%s] input=%d, output=%d, select=%.2f, elapsed=%s, velo=%.1f/s",
										partIdx, tag, getHandlerString(), m_inCount, m_outCount,
										selectivity, m_watch.getElapsedMillisString(), velo);
			getLogger().info(msg);
		}
	}
}
