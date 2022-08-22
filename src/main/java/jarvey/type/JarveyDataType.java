package jarvey.type;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Maps;

import jarvey.support.typeexpr.JarveyTypeParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JarveyDataType implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected final DataType m_sparkType;
	
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
	
	JarveyDataType(DataType sparkType) {
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

	/**
	 * Parse a type string expression and returns JarveyDataType object.
	 * 
	 * @param typeExpr	type string.
	 * @return	JarveyDataType object.
	 */
	public static JarveyDataType fromString(String typeExpr) {
		return JarveyTypeParser.parseTypeExpr(typeExpr);
	}
	
	/**
	 * Get JavaDataType that corresponding to the given java class.
	 *
	 * @param cls	Java class
	 * @return	JavaDataType for this java class.
	 */
	public static JarveyDataType fromJavaClass(Class<?> cls) {
		if ( Geometry.class.isAssignableFrom(cls) ) {
			return GeometryType.fromJavaClass(cls, 0);
		}
		
		JarveyDataType jtype = CLASS_TO_TYPE.get(cls);
		if ( jtype != null ) {
			return jtype;
		}
		
		Class<?> elmCls = cls.getComponentType();
		if ( elmCls != null ) {
			JarveyDataType elmJType = fromJavaClass(elmCls);
			return ArrayType.of(elmJType, true);
		}
		
		throw new IllegalArgumentException("unknown Java class: " + cls);
	}
	
	public static final Map<Class<?>, JarveyDataType> CLASS_TO_TYPE = Maps.newHashMap();
	static {
		CLASS_TO_TYPE.put(String.class, JarveyDataTypes.StringType);
		CLASS_TO_TYPE.put(Long.class, JarveyDataTypes.LongType);
		CLASS_TO_TYPE.put(Integer.class, JarveyDataTypes.IntegerType);
		CLASS_TO_TYPE.put(Short.class, JarveyDataTypes.ShortType);
		CLASS_TO_TYPE.put(Byte.class, JarveyDataTypes.ByteType);
		CLASS_TO_TYPE.put(Double.class, JarveyDataTypes.DoubleType);
		CLASS_TO_TYPE.put(Float.class, JarveyDataTypes.FloatType);
		CLASS_TO_TYPE.put(Byte[].class, JarveyDataTypes.BinaryType);
		CLASS_TO_TYPE.put(Boolean.class, JarveyDataTypes.BooleanType);
		CLASS_TO_TYPE.put(Date.class, JarveyDataTypes.DateType);
		CLASS_TO_TYPE.put(java.util.Date.class, JarveyDataTypes.DateType);
		CLASS_TO_TYPE.put(Timestamp.class, JarveyDataTypes.TimestampType);
		
		CLASS_TO_TYPE.put(Envelope.class, JarveyDataTypes.Envelope_Type);
	};
}