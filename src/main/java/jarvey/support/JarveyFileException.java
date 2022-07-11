package jarvey.support;

import jarvey.JarveyRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyFileException extends JarveyRuntimeException {
	private static final long serialVersionUID = 1L;

	public JarveyFileException(Throwable cause) {
		super(cause);
	}

	public JarveyFileException(String details) {
		super(details);
	}

	public JarveyFileException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
