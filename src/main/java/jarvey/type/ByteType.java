package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ByteType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final ByteType SINGLETON = new ByteType();
	public static final ByteType get() {
		return SINGLETON;
	}
	
	public ByteType() {
		super(DataTypes.ByteType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Byte.class;
	}
	
	@Override
	public String toString() {
		return "Byte";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != ByteType.class ) {
			return false;
		}
		
		return true;
	}
}
