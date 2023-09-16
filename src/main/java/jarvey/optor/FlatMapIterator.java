package jarvey.optor;

import java.util.Collections;
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
public abstract class FlatMapIterator<IN,OUT> implements Iterator<OUT>, LoggerSettable {
	private final Iterator<IN> m_inputIter;
	private Iterator<OUT> m_mappeds = Collections.emptyIterator();
	private OUT m_next;
	private Logger m_logger;
	
	private StopWatch m_watch;
	private long m_reportIntervalMills;
	private long m_lastReportedMillis;
	private int m_inCount = 0;
	private int m_outCount = 0;
	
	protected abstract Iterator<OUT> apply(IN input);
	protected void onIteratorFinished() { }
	protected String getHandlerString() { return ""; }
	
	public FlatMapIterator(Iterator<IN> iter, long reportIntervalMillis) {
		m_watch = StopWatch.start();
		m_lastReportedMillis = m_watch.getElapsedInMillis();
		m_reportIntervalMills = reportIntervalMillis;

		m_inputIter = iter;
		m_logger = LoggerFactory.getLogger(getClass());
		
		m_next = lookAhead();
	}
	
	public FlatMapIterator(Iterator<IN> iter) {
		this(iter, UnitUtils.parseDurationMillis("1m"));
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

	@Override
	public OUT next() {
		if ( m_next == null ) {
			throw new NoSuchElementException();
		}
		OUT next = m_next;
		m_next = lookAhead();
		
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
	
	private OUT lookAhead() {
		while ( true ) {
			if ( m_mappeds.hasNext() ) {
				++m_outCount;
				reportProgress(m_watch);
				
				return m_mappeds.next();
			}
			
			if ( m_inputIter.hasNext() ) {
				IN input = m_inputIter.next();
				++m_inCount;
				reportProgress(m_watch);
				
				m_mappeds = apply(input);
			}
			else {
				handleEndOfIterator();
				return null;
			}
		}
	}
	
	private void handleEndOfIterator() {
		Unchecked.runOrIgnore(this::onIteratorFinished);
		printProgress("    done");
	}
	
	private void reportProgress(StopWatch watch) {
		long now = watch.getElapsedInMillis();
		if ( m_reportIntervalMills > 0
			&& (now - m_lastReportedMillis) >= m_reportIntervalMills ) {
			printProgress("progress");
			m_lastReportedMillis = now;
		}
	}
	
	private void printProgress(String tag) {
		if ( getLogger().isInfoEnabled() ) {
			int partIdx = TaskContext.getPartitionId();
			double velo = m_inCount / m_watch.getElapsedInFloatingSeconds();
			String msg = String.format("[%4d] %s: [%s] input=%d, output=%d, elapsed=%s, velo=%.1f/s",
										partIdx, tag, getHandlerString(), m_inCount, m_outCount,
										m_watch.getElapsedMillisString(), velo);
			getLogger().info(msg);
		}
	}
}
