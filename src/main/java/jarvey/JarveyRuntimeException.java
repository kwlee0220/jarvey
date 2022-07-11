package jarvey;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public JarveyRuntimeException() {
		super();
	}

	public JarveyRuntimeException(String details) {
		super(details);
	}

	public JarveyRuntimeException(Throwable cause) {
		super(cause);
	}

	public JarveyRuntimeException(String details, Throwable cause) {
		super(details, cause);
	}
	
	public String getFullMessage() {
		String detailsMsg = (getMessage() != null) ? String.format(": %s", getMessage()) : "";
		String causeMsg = (getCause() != null) 	? String.format(", cause=%s", getCause()) : "";
		
		return String.format("%s%s%s", getClass().getName(), detailsMsg, causeMsg);
	}
}
