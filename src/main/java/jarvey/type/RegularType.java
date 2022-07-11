/**
 * 
 */
package jarvey.type;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import com.google.common.collect.Maps;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class RegularType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	
	public static final RegularType of(DataType sparkType) {
		return new RegularType(sparkType);
	}
	
	private RegularType(DataType type) {
		super(type);
		
		if ( type instanceof ArrayType ) {
			throw new IllegalArgumentException("invalid regular datatype: " + type);
		}
	}
	
	@Override
	public Class<?> getJavaClass() {
		Class<?> cls = TYPE_TO_CLASSES.get(getSparkType());
		if ( cls != null ) {
			return cls;
		}
		
		throw new IllegalArgumentException("invalid Spark DataType" + getSparkType());
	}

	@Override
	public Object serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		
		Function<Object,Object> serializer = SERIALIZERS.get(getSparkType());
		return serializer != null ? serializer.apply(value) : value;
	}
	
	@Override
	public Object deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		
		Function<Object,Object> deser = DESERIALIZERS.get(getSparkType());
		if ( deser != null ) {
			return deser.apply(value);
		}
		else {
			return value;
		}
	}

//	private static final Pattern ARRAY_SUBTYPE_PATTERN = Pattern.compile("\\<\\s*(\\S+)\\s*(,\\S+)\\>");
//	public static RegularType fromString(String typeExpr) {
//		typeExpr = typeExpr.trim().toLowerCase();
//		if ( typeExpr.startsWith("array") ) {
//			String subTypeName = typeExpr.substring("array".length());
//			Matcher matcher = ARRAY_SUBTYPE_PATTERN.matcher(subTypeName);
//			if ( !matcher.find() ) {
//				throw new IllegalArgumentException(String.format("invalid array type: '%s'", typeExpr));
//			}
//			
//			int groupCnt = matcher.groupCount();
//			if ( groupCnt >= 1 ) {
//				subTypeName = matcher.group(1);
//				JarveyDataType elmType = JarveyDataType.fromString(subTypeName);
//				DataType arrType = DataTypes.createArrayType(elmType.getSparkType(), true);
//				return new RegularType(arrType);
//			}
//			
//			throw new IllegalArgumentException(String.format("invalid array type: '%s'", typeExpr));
//		}
//		
//		DataType dtype = TYPEEXPR_TO_TYPE.get(typeExpr);
//		if ( dtype != null ) {
//			return new RegularType(dtype);
//		}
//		
//		throw new IllegalArgumentException(String.format("invalid type: '%s'", typeExpr));
//	}
	
	@Override
	public String toString() {
		String name = TYPE_NAMES.get(m_sparkType);
		if ( name != null ) {
			return name;
		}
		
		throw new IllegalArgumentException("unknown data-type: " + m_sparkType);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof RegularType) ) {
			return false;
		}
		
		RegularType other = (RegularType)obj;
		return getSparkType().equals(other.getSparkType());
	}
	
	@Override
	public int hashCode() {
		return getSparkType().hashCode();
	}
	
	public static final Map<DataType, String> TYPE_NAMES = Maps.newHashMap();
	public static final Map<DataType, Class<?>> TYPE_TO_CLASSES = Maps.newHashMap();
	public static final Map<String, DataType> TYPEEXPR_TO_TYPE = Maps.newHashMap();
	public static final Map<DataType, Function<Object,Object>> SERIALIZERS = Maps.newHashMap();
	public static final Map<DataType, Function<Object,Object>> DESERIALIZERS = Maps.newHashMap();
	static {
		TYPE_NAMES.put(DataTypes.StringType, "String");
		TYPE_NAMES.put(DataTypes.LongType, "Long");
		TYPE_NAMES.put(DataTypes.IntegerType, "Integer");
		TYPE_NAMES.put(DataTypes.ShortType, "Short");
		TYPE_NAMES.put(DataTypes.ByteType, "Byte");
		TYPE_NAMES.put(DataTypes.DoubleType, "Double");
		TYPE_NAMES.put(DataTypes.FloatType, "Float");
		TYPE_NAMES.put(DataTypes.BinaryType, "Binary");
		TYPE_NAMES.put(DataTypes.BooleanType, "Boolean");
		TYPE_NAMES.put(DataTypes.DateType, "Date");
		TYPE_NAMES.put(DataTypes.TimestampType, "Timestamp");
		TYPE_NAMES.put(DataTypes.CalendarIntervalType, "CalendarInterval");
		
		TYPE_TO_CLASSES.put(DataTypes.StringType, String.class);
		TYPE_TO_CLASSES.put(DataTypes.LongType, Long.class);
		TYPE_TO_CLASSES.put(DataTypes.IntegerType, Integer.class);
		TYPE_TO_CLASSES.put(DataTypes.ShortType, Short.class);
		TYPE_TO_CLASSES.put(DataTypes.ByteType, Byte.class);
		TYPE_TO_CLASSES.put(DataTypes.DoubleType, Double.class);
		TYPE_TO_CLASSES.put(DataTypes.FloatType, Float.class);
		TYPE_TO_CLASSES.put(DataTypes.BinaryType, Byte[].class);
		TYPE_TO_CLASSES.put(DataTypes.BooleanType, Boolean.class);
		TYPE_TO_CLASSES.put(DataTypes.DateType, Date.class);
		TYPE_TO_CLASSES.put(DataTypes.TimestampType, Timestamp.class);
		
		TYPEEXPR_TO_TYPE.put("string", DataTypes.StringType);
		TYPEEXPR_TO_TYPE.put("int", DataTypes.StringType);
		TYPEEXPR_TO_TYPE.put("float", DataTypes.FloatType);
		TYPEEXPR_TO_TYPE.put("double", DataTypes.DoubleType);
		TYPEEXPR_TO_TYPE.put("long", DataTypes.LongType);
		TYPEEXPR_TO_TYPE.put("binary", DataTypes.BinaryType);
		TYPEEXPR_TO_TYPE.put("short", DataTypes.ShortType);
		TYPEEXPR_TO_TYPE.put("byte", DataTypes.ByteType);
		TYPEEXPR_TO_TYPE.put("boolean", DataTypes.BooleanType);
		TYPEEXPR_TO_TYPE.put("timestamp", DataTypes.TimestampType);
		TYPEEXPR_TO_TYPE.put("date", DataTypes.DateType);
		TYPEEXPR_TO_TYPE.put("float", DataTypes.FloatType);

		SERIALIZERS.put(DataTypes.StringType, str -> UTF8String.fromString(str.toString()));
		SERIALIZERS.put(DataTypes.IntegerType, DataUtils::asInt);
		SERIALIZERS.put(DataTypes.LongType, DataUtils::asLong);
		SERIALIZERS.put(DataTypes.ShortType, DataUtils::asShort);
		SERIALIZERS.put(DataTypes.ByteType, DataUtils::asByte);
		SERIALIZERS.put(DataTypes.DoubleType, DataUtils::asDouble);
		SERIALIZERS.put(DataTypes.FloatType, DataUtils::asFloat);
		SERIALIZERS.put(DataTypes.BooleanType, DataUtils::asBoolean);
		SERIALIZERS.put(DataTypes.DateType, obj -> {
			if ( obj instanceof java.sql.Date ) {
				long millis = ((java.sql.Date)obj).getTime();
				return (int)(millis / (1000 * 60 * 60 * 24)) + 1;
			}
			else if ( obj instanceof java.util.Date ) {
				long millis = ((java.util.Date)obj).getTime();
				return (int)(millis / (1000 * 60 * 60 * 24)) + 1;
			}
			else if ( obj == null ) {
				return null;
			}
			else {
				throw new IllegalArgumentException("invalid Date data: obj=" + obj);
			}
		});
	}
}
