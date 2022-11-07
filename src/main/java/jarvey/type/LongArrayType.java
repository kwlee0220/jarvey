package jarvey.type;

import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class LongArrayType extends JarveyArrayType {
	private static final long serialVersionUID = 1L;
	
	public static final LongArrayType of(boolean elmNullable) {
		return new LongArrayType(elmNullable);
	}
	
	protected LongArrayType(boolean elmNullable) {
  		super(JarveyDataTypes.Long_Type, elmNullable);
	}

	@Override
	public Class<?> getJavaClass() {
		return Long[].class;
	}

	@Override
	public Object serialize(Object array) {
		return array;
	}
	
	@Override
	public Long[] deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		
		WrappedArray<?> array = (WrappedArray<?>)value;
		Long[] output = new Long[array.length()];
		for ( int i =0; i < output.length; ++i ) {
			output[i] = (Long)array.apply(i);
		}
		
		return output;
	}
	
	@Override
	public String toString() {
		String nullableStr = getElementNullable() ? "nullable" : "non-null";
		return String.format("LongArray<%s>", nullableStr);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof LongArrayType) ) {
			return false;
		}
		
		LongArrayType other = (LongArrayType)obj;
		return getElementNullable() == other.getElementNullable();
	}
	
	public static Object[] unwrap(WrappedArray<?> array) {
		if ( array != null ) {
			Object[] unwrapped = new Object[array.size()];
			for ( int i =0; i < unwrapped.length; ++i ) {
				unwrapped[i] = array.apply(i);
			}
			return unwrapped;
		}
		else {
			return null;
		}
	}
}
