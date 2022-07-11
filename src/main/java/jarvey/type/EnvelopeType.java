/**
 * 
 */
package jarvey.type;

import java.util.Objects;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.locationtech.jts.geom.Envelope;

import jarvey.datasource.DatasetException;
import scala.collection.mutable.WrappedArray;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	private static final DataType SPARK_TYPE = DataTypes.createArrayType(DataTypes.DoubleType);
	
	private static final EnvelopeType ENVL_TYPE = new EnvelopeType();
	public static final EnvelopeType get() {
		return ENVL_TYPE;
	}
	
	protected EnvelopeType() {
		super(SPARK_TYPE);
	}

	@Override
	public Class<?> getJavaClass() {
		return Envelope.class;
	}
	
	@Override
	public Object serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof Envelope ) {
			return EnvelopeValue.toCoordinates((Envelope)value);
		}
		else {
			throw new DatasetException("invalid Envelope: " + value);
		}
	}

	@Override
	public Object deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof WrappedArray ) {
			return EnvelopeValue.toEnvelope((WrappedArray<Double>)value);
		}
		else {
			throw new DatasetException("invalid serialized value for Envelope: " + value);
		}
	}
	
	@Override
	public String toString() {
		return "Envelope";
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof EnvelopeType) ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getSparkType());
	}
}
