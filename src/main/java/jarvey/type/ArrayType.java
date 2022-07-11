/**
 * 
 */
package jarvey.type;

import java.lang.reflect.Array;
import java.util.Objects;

import org.apache.spark.sql.types.DataTypes;

import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class ArrayType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	
	private final JarveyDataType m_elmType;
	private final boolean m_elmNullable;
	
	public static final ArrayType of(JarveyDataType elmType, boolean elmNullable) {
		return new ArrayType(elmType, elmNullable);
	}
	
	private ArrayType(JarveyDataType elmType, boolean elmNullable) {
  		super(DataTypes.createArrayType(elmType.getSparkType(), elmNullable));
		
		m_elmType = elmType;
		m_elmNullable = elmNullable;
	}

	@Override
	public Class<?> getJavaClass() {
		return Array.newInstance(m_elmType.getJavaClass(), 0).getClass();
	}

	@Override
	public Object serialize(Object array) {
		if ( array == null ) {
			return null;
		}
		
		int length = Array.getLength(array);
		Object[] serializeds = new Object[length];
		for ( int i =0; i < length; ++i ) {
			Object elm = Array.get(array, i);
			serializeds[i] = m_elmType.serialize(elm);
		}
		
		return serializeds;
	}
	
	@Override
	public Object deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		
		WrappedArray<?> array = (WrappedArray<?>)value;
		Object[] output = new Object[array.length()];
		for ( int i =0; i < output.length; ++i ) {
			output[i] = m_elmType.deserialize(array.apply(i));
		}
		
		return output;
	}
	
	@Override
	public String toString() {
		String elmTypeStr = m_elmType.toString();
		String nullableStr = m_elmNullable ? "nullable" : "non-null";
		return String.format("Array<%s,%s>", elmTypeStr, nullableStr);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof ArrayType) ) {
			return false;
		}
		
		ArrayType other = (ArrayType)obj;
		return m_elmType.equals(other.m_elmType) && m_elmNullable == other.m_elmNullable;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(m_elmType, m_elmNullable);
	}
	
	public static double[] unwrapDoubleArray(WrappedArray<Double> array) {
		if ( array != null ) {
			double[] unwrapped = new double[array.size()];
			for ( int i =0; i < unwrapped.length; ++i ) {
				unwrapped[i] = array.apply(i);
			}
			return unwrapped;
		}
		else {
			return null;
		}
	}
	
	public static String[] unwrapStringArray(WrappedArray<String> array) {
		if ( array != null ) {
			String[] unwrapped = new String[array.size()];
			for ( int i =0; i < unwrapped.length; ++i ) {
				unwrapped[i] = array.apply(i);
			}
			return unwrapped;
		}
		else {
			return null;
		}
	}
	
	public static Long[] unwrapLongArray(WrappedArray<Long> array) {
		if ( array != null ) {
			Long[] unwrapped = new Long[array.size()];
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
