package jarvey.type;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JarveyDataType implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected final DataType m_sparkType;
	
	public Object serializeToInternal(Object value) {
		return serialize(value);
	}
	
	/**
	 * Converts the input value into a serialized one.
	 * 
	 * @param value		Java object.
	 * @return	a Spark-serialized value.
	 */
	public abstract Object serialize(Object value);
	
	/**
	 * Converts the input Spark-serialized value into a Java object.
	 * 
	 * @param value		Spark-serialized object.
	 * @return	a Java object.
	 */
	public abstract Object deserialize(Object value);
	
	/**
	 * Returns java class for this jarvey datatype.
	 *
	 * @return	Java class.
	 */
	public abstract Class<?> getJavaClass();
	
	public JarveyDataType(DataType sparkType) {
		m_sparkType = sparkType;
	}
	
	/**
	 * returns Spark datatype.
	 * 
	 * @return Spark datatype.
	 */
	public DataType getSparkType() {
		return m_sparkType;
	}

	/**
	 * Returns whether this is a regular type or not.
	 * 
	 * @return {@code true} if this is a regular type, {@code false} otherwise.
	 */
	public boolean isRegularType() {
		return this instanceof RegularType;
	}

	/**
	 * Returns whether this is a Geometry type or not.
	 * 
	 * @return {@code true} if this is a geometry type, {@code false} otherwise.
	 */
	public boolean isGeometryType() {
		return this instanceof GeometryType;
	}

	/**
	 * Returns whether this is a Envelope type or not.
	 * 
	 * @return {@code true} if this is a Envelope type, {@code false} otherwise.
	 */
	public boolean isEnvelopeType() {
		return this instanceof EnvelopeType;
	}

	/**
	 * Cast this type as a Regular type.
	 * 
	 * @return {@link RegularType}
	 * @throws IllegalStateException	if this is not a Regular type.
	 */
	public RegularType asRegularType() {
		if ( this instanceof RegularType ) {
			return (RegularType)this;
		}
		else {
			throw new IllegalStateException("not RegularType: " + this);
		}
	}

	/**
	 * Cast this type as a Geometry type object.
	 * 
	 * @return {@link GeometryType}
	 * @throws IllegalStateException	if this is not a Geometry type.
	 */
	public GeometryType asGeometryType() {
		if ( this instanceof GeometryType ) {
			return (GeometryType)this;
		}
		else {
			throw new IllegalStateException("not GeometryType: " + this);
		}
	}

	/**
	 * Cast this type as a EnvelopeType object.
	 * 
	 * @return {@link EnvelopeType}
	 * @throws IllegalStateException	if this is not a EnvelopeType.
	 */
	public EnvelopeType asEnvelopeType() {
		if ( this instanceof EnvelopeType ) {
			return (EnvelopeType)this;
		}
		else {
			throw new IllegalStateException("not EnvelopeType: " + this);
		}
	}
}