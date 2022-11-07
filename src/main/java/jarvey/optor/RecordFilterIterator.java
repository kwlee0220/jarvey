package jarvey.optor;

import java.util.Iterator;

import jarvey.support.RecordLite;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class RecordFilterIterator implements Iterator<RecordLite> {
	private final Iterator<RecordLite> m_iter;
	private RecordLite m_next;
	
	protected abstract boolean test(RecordLite input);
	
	public RecordFilterIterator(Iterator<RecordLite> iter) {
		m_iter = iter;
		m_next = lookAhead();
	}

	@Override
	public boolean hasNext() {
		return m_next != null;
	}

	@Override
	public RecordLite next() {
		RecordLite next = m_next;
		m_next = lookAhead();
		
		return next;
	}
	
	private RecordLite lookAhead() {
		while ( m_iter.hasNext() ) {
			RecordLite input = m_iter.next();
			if ( test(input) ) {
				return input;
			}
		}
		
		return null;
	}
}
