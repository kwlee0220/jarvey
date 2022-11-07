package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DateType extends RegularType {
	private static final long serialVersionUID = 1L;
	private static final long DAY_MILLIS = (1000 * 60 * 60 * 24);
	
	private static final DateType SINGLETON = new DateType();
	public static final DateType get() {
		return SINGLETON;
	}
	
	public DateType() {
		super(DataTypes.DateType);
	}

	@Override
	public Class<?> getJavaClass() {
		return java.sql.Date.class;
	}

	@Override
	public Object serializeToInternal(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof java.sql.Date ) {
			long millis = ((java.sql.Date)value).getTime();
			return (int)(millis / DAY_MILLIS) + 1;
		}
		else if ( value instanceof java.util.Date ) {
			long millis = ((java.util.Date)value).getTime();
			return (int)(millis / DAY_MILLIS) + 1;
		}
		else {
			throw new IllegalArgumentException("invalid Date data: obj=" + value);
		}
	}
	
	@Override
	public String toString() {
		return "Date";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != DateType.class ) {
			return false;
		}
		
		return true;
	}
}
