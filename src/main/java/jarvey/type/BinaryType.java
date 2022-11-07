package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final BinaryType SINGLETON = new BinaryType();
	public static final BinaryType get() {
		return SINGLETON;
	}
	
	public BinaryType() {
		super(DataTypes.BinaryType);
	}

	@Override
	public Class<?> getJavaClass() {
		return byte[].class;
	}
	
	@Override
	public String toString() {
		return "Binary";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != BinaryType.class ) {
			return false;
		}
		
		return true;
	}
}
