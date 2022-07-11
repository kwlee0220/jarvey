package jarvey.datasource;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DatasetOperationException extends DatasetException {
	private static final long serialVersionUID = 1L;

	public DatasetOperationException(Throwable cause) {
		super(cause);
	}

	public DatasetOperationException(String details) {
		super(details);
	}

	public DatasetOperationException(String details, Throwable cause) {
		super(details + ", cause=" + cause);
	}
}
