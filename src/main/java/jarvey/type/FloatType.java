package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FloatType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final FloatType SINGLETON = new FloatType();
	public static final FloatType get() {
		return SINGLETON;
	}
	
	public FloatType() {
		super(DataTypes.FloatType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Float.class;
	}
	
	@Override
	public String toString() {
		return "Float";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != FloatType.class ) {
			return false;
		}
		
		return true;
	}
}
