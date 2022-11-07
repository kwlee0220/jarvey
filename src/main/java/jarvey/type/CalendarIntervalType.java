package jarvey.type;

import java.sql.Timestamp;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalendarIntervalType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final CalendarIntervalType SINGLETON = new CalendarIntervalType();
	public static final CalendarIntervalType get() {
		return SINGLETON;
	}
	
	public CalendarIntervalType() {
		super(DataTypes.CalendarIntervalType);
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
		else if ( obj == null || obj.getClass() != CalendarIntervalType.class ) {
			return false;
		}
		
		return true;
	}
}
