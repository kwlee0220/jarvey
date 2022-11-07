package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DoubleType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final DoubleType SINGLETON = new DoubleType();
	public static final DoubleType get() {
		return SINGLETON;
	}
	
	public DoubleType() {
		super(DataTypes.DoubleType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Double.class;
	}
	
	@Override
	public String toString() {
		return "Double";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != DoubleType.class ) {
			return false;
		}
		
		return true;
	}
}
