package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LongType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final LongType SINGLETON = new LongType();
	public static final LongType get() {
		return SINGLETON;
	}
	
	public LongType() {
		super(DataTypes.LongType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Long.class;
	}
	
	@Override
	public String toString() {
		return "Long";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != LongType.class ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return DataTypes.LongType.hashCode();
	}

}
