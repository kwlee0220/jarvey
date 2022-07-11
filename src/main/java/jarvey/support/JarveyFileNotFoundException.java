package jarvey.support;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyFileNotFoundException extends JarveyFileException {
	private static final long serialVersionUID = 1L;
	
	public JarveyFileNotFoundException(String details) {
		super(details);
	}

	public JarveyFileNotFoundException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
