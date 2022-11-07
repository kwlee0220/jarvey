package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IntegerType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final IntegerType SINGLETON = new IntegerType();
	public static final IntegerType get() {
		return SINGLETON;
	}
	
	public IntegerType() {
		super(DataTypes.IntegerType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Integer.class;
	}
	
	@Override
	public String toString() {
		return "Integer";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != IntegerType.class ) {
			return false;
		}
		
		return true;
	}
}
