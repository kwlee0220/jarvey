package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BooleanType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final BooleanType SINGLETON = new BooleanType();
	public static final BooleanType get() {
		return SINGLETON;
	}
	
	public BooleanType() {
		super(DataTypes.BooleanType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Boolean.class;
	}
	
	@Override
	public String toString() {
		return "Boolean";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != BooleanType.class ) {
			return false;
		}
		
		return true;
	}
}
