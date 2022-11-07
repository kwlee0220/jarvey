package jarvey.type;

import org.apache.spark.ml.linalg.VectorUDT;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class VectorType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	
	private static final VectorUDT SPARK_VECTOR_UDT = new VectorUDT();
	private static final VectorType SINGLETON = new VectorType();
	public static final VectorType get() {
		return SINGLETON;
	}
	
	public VectorType() {
		super(SPARK_VECTOR_UDT);
	}

	@Override
	public Class<?> getJavaClass() {
		return VectorUDT.class;
	}
	
	@Override
	public String toString() {
		return "Vector";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != VectorType.class ) {
			return false;
		}
		
		return true;
	}

	@Override
	public Object serialize(Object value) {
		return value;
	}

	@Override
	public Object deserialize(Object value) {
		return value;
	}
}
