package jarvey.type;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import jarvey.datasource.DatasetException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StringType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final StringType SINGLETON = new StringType();
	public static final StringType get() {
		return SINGLETON;
	}
	
	public StringType() {
		super(DataTypes.StringType);
	}

	@Override
	public Class<?> getJavaClass() {
		return String.class;
	}
	
	public Object serializeToInternal(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof String ) {
			return UTF8String.fromString((String)value);
		}
		else {
			throw new DatasetException("invalid String java value: " + value);
		}
	}
	
	@Override
	public String toString() {
		return "String";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != StringType.class ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return DataTypes.StringType.hashCode();
	}

}
