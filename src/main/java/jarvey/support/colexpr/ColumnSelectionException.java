package jarvey.support.colexpr;

import jarvey.JarveyRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnSelectionException extends JarveyRuntimeException {
	private static final long serialVersionUID = 1L;

	public ColumnSelectionException(String details) {
		super(details);
	}

}
