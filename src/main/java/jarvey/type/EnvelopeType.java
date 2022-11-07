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
	public  Double[] serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof Envelope ) {
			Envelope envl = (Envelope)value;
			return new Double[]{ envl.getMinX(), envl.getMaxX(), envl.getMinY(), envl.getMaxY()};
		}
		else {
			throw new DatasetException("invalid Envelope: " + value);
		}
	}

	@Override
	public Envelope deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof WrappedArray ) {
			@SuppressWarnings("unchecked")
			WrappedArray<Double> wrapped = (WrappedArray<Double>)value;
			return newInstance(wrapped.apply(0), wrapped.apply(1), wrapped.apply(2), wrapped.apply(3));
		}
		else if ( value.getClass() == Double[].class ) {
			Double[] wrapped = (Double[])value;
			return newInstance(wrapped[0], wrapped[1], wrapped[2], wrapped[3]);
		}
		else if ( value.getClass() == double[].class ) {
			double[] wrapped = (double[])value;
			return newInstance(wrapped[0], wrapped[1], wrapped[2], wrapped[3]);
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
	
	private static Envelope newInstance(double minX, double maxX, double minY, double maxY) {
		if ( minX > maxX ) {
			return new Envelope();
		}
		else {
			return new Envelope(minX, maxX, minY, maxY);
		}
	}
}
