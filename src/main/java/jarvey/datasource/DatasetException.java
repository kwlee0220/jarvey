package jarvey.datasource;

import jarvey.JarveyRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DatasetException extends JarveyRuntimeException {
	private static final long serialVersionUID = 1L;

	public DatasetException(Throwable cause) {
		super(cause);
	}

	public DatasetException(String details) {
		super(details);
	}

	public DatasetException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
