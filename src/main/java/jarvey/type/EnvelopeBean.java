package jarvey.type;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;

import scala.collection.mutable.WrappedArray;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeBean implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final DataType DATA_TYPE = DataTypes.createArrayType(DataTypes.DoubleType);
	public static final Encoder<EnvelopeBean> ENCODER = Encoders.bean(EnvelopeBean.class);

	private Double[] m_coords = null;
	private transient Envelope m_envl = null;
	
	public EnvelopeBean() {
		this(new Envelope());
	}
	
	public EnvelopeBean(Envelope envl) {
		this(envl, null);
	}
	
	public EnvelopeBean(Double[] coords) {
		this(null, coords);
	}
	
	public EnvelopeBean(Envelope envl, Double[] coords) {
		m_envl = envl;
		m_coords = coords;
	}
	
	public Double[] getCoordinates() {
		if ( m_coords == null ) {
			m_coords = serialize(m_envl);
		}
		return m_coords;
	}
	
	public void setCoordinates(Double[] coords) {
		m_coords = coords;
		m_envl = null;
	}
	
	public Envelope asEnvelope() {
		if ( m_envl == null ) {
			m_envl = deserialize(m_coords);
		}
		return m_envl;
	}
	
	public void update(Envelope envl) {
		m_envl = envl;
		m_coords = null;
	}
	
	public static Double[] serialize(Envelope envl) {
		if ( envl != null ) {
			return new Double[]{ envl.getMinX(), envl.getMaxX(), envl.getMinY(), envl.getMaxY()};
		}
		else {
			return null;
		}
	}
	
	public static Envelope deserialize(Object coords) {
		if ( coords == null ) {
			return null;
		}
		else if ( coords instanceof WrappedArray ) {
			@SuppressWarnings("unchecked")
			WrappedArray<Double> wrapped = (WrappedArray<Double>)coords;
			return newInstance(wrapped.apply(0), wrapped.apply(1), wrapped.apply(2), wrapped.apply(3));
		}
		else if ( coords.getClass() == double[].class ) {
			double[] wrapped = (double[])coords;
			return newInstance(wrapped[0], wrapped[1], wrapped[2], wrapped[3]);
		}
		else if ( coords.getClass() == Double[].class ) {
			Double[] wrapped = (Double[])coords;
			return newInstance(wrapped[0], wrapped[1], wrapped[2], wrapped[3]);
		}
		else {
			throw new AssertionError();
		}
	}
	
	public static EnvelopeType tryUnwrapStructType(StructType type) {
		DataType fieldType = ((StructType)type).fields()[0].dataType();
		if ( fieldType instanceof ArrayType ) {
			DataType elmType = ((ArrayType)fieldType).elementType();
			if ( elmType instanceof DoubleType ) {
				return JarveyDataTypes.Envelope_Type;
			}
		}
		
		return null;
	}
	
	private static Envelope newInstance(double minX, double maxX, double minY, double maxY) {
		if ( minX > maxX ) {
			return new Envelope();
		}
		else {
			return new Envelope(minX, maxX, minY, maxY);
		}
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final Double[] m_coords;
		
		private SerializationProxy(EnvelopeBean bean) {
			m_coords = bean.getCoordinates();
		}
		
		private Object readResolve() {
			return new EnvelopeBean(m_coords);
		}
	}
}
