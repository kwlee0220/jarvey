package jarvey.type;

import java.sql.Timestamp;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TimestampType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final TimestampType SINGLETON = new TimestampType();
	public static final TimestampType get() {
		return SINGLETON;
	}
	
	public TimestampType() {
		super(DataTypes.TimestampType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Timestamp.class;
	}
	
	@Override
	public String toString() {
		return "Timestamp";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != TimestampType.class ) {
			return false;
		}
		
		return true;
	}
}
