package jarvey.type;

import org.apache.spark.sql.types.DataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShortType extends RegularType {
	private static final long serialVersionUID = 1L;
	
	private static final ShortType SINGLETON = new ShortType();
	public static final ShortType get() {
		return SINGLETON;
	}
	
	public ShortType() {
		super(DataTypes.ShortType);
	}

	@Override
	public Class<?> getJavaClass() {
		return Short.class;
	}
	
	@Override
	public String toString() {
		return "Short";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != ShortType.class ) {
			return false;
		}
		
		return true;
	}
}
