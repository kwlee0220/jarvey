/**
 * 
 */
package jarvey.support;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Maps;

import jarvey.type.EnvelopeValue;
import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TypeUtils {
	private TypeUtils() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
//	private static final DataType DoubleArrayType = DataTypes.createArrayType(DataTypes.DoubleType);
//	private static final DataType StringArrayType = DataTypes.createArrayType(DataTypes.StringType);
//	
//	public static final Map<DataType, String> TYPE_NAMES = Maps.newHashMap();
	public static final Map<Class<?>, DataType> CLASS_TO_TYPES = Maps.newHashMap();
//	public static final Map<DataType, Class<?>> TYPE_TO_CLASSES = Maps.newHashMap();
//	public static final Map<String, DataType> TYPEEXPR_TO_TYPE = Maps.newHashMap();
//	public static final Map<DataType, Function<Object,Object>> SERIALIZERS = Maps.newHashMap();
//	public static final Map<DataType, Function<WrappedArray,Object>> DESERIALIZERS = Maps.newHashMap();
	static {
//		TYPE_NAMES.put(DataTypes.StringType, "string");
//		TYPE_NAMES.put(DataTypes.DoubleType, "double");
//		TYPE_NAMES.put(DataTypes.FloatType, "float");
//		TYPE_NAMES.put(DataTypes.LongType, "long");
//		TYPE_NAMES.put(DataTypes.IntegerType, "int");
//		TYPE_NAMES.put(DataTypes.ShortType, "short");
//		TYPE_NAMES.put(DataTypes.ByteType, "byte");
//		TYPE_NAMES.put(DataTypes.BooleanType, "boolean");
//		TYPE_NAMES.put(DataTypes.BinaryType, "binary");
//		TYPE_NAMES.put(DataTypes.DateType, "date");
//		TYPE_NAMES.put(DataTypes.TimestampType, "timestamp");
		
		CLASS_TO_TYPES.put(String.class, DataTypes.StringType);
		CLASS_TO_TYPES.put(Long.class, DataTypes.LongType);
		CLASS_TO_TYPES.put(Integer.class, DataTypes.IntegerType);
		CLASS_TO_TYPES.put(Short.class, DataTypes.ShortType);
		CLASS_TO_TYPES.put(Byte.class, DataTypes.ByteType);
		CLASS_TO_TYPES.put(Double.class, DataTypes.DoubleType);
		CLASS_TO_TYPES.put(Float.class, DataTypes.FloatType);
		CLASS_TO_TYPES.put(Byte[].class, DataTypes.BinaryType);
		CLASS_TO_TYPES.put(Boolean.class, DataTypes.BooleanType);
		CLASS_TO_TYPES.put(Date.class, DataTypes.DateType);
		CLASS_TO_TYPES.put(java.util.Date.class, DataTypes.DateType);
		CLASS_TO_TYPES.put(Timestamp.class, DataTypes.TimestampType);
		CLASS_TO_TYPES.put(Geometry.class, GeometryType.DATA_TYPE);
		CLASS_TO_TYPES.put(Envelope.class, EnvelopeValue.DATA_TYPE);
		
//		TYPE_TO_CLASSES.put(DataTypes.StringType, String.class);
//		TYPE_TO_CLASSES.put(DataTypes.DoubleType, Double.class);
//		TYPE_TO_CLASSES.put(DataTypes.FloatType, Float.class);
//		TYPE_TO_CLASSES.put(DataTypes.LongType, Long.class);
//		TYPE_TO_CLASSES.put(DataTypes.IntegerType, Integer.class);
//		TYPE_TO_CLASSES.put(DataTypes.ShortType, Short.class);
//		TYPE_TO_CLASSES.put(DataTypes.ByteType, Byte.class);
//		TYPE_TO_CLASSES.put(DataTypes.BooleanType, Boolean.class);
//		TYPE_TO_CLASSES.put(DataTypes.BinaryType, Byte[].class);
//		TYPE_TO_CLASSES.put(DataTypes.DateType, Date.class);
//		TYPE_TO_CLASSES.put(DataTypes.TimestampType, Timestamp.class);
//		TYPE_TO_CLASSES.put(StringArrayType, String[].class);
//		TYPE_TO_CLASSES.put(DoubleArrayType, double[].class);
//		
//		TYPEEXPR_TO_TYPE.put("string", DataTypes.StringType);
//		TYPEEXPR_TO_TYPE.put("int", DataTypes.StringType);
//		TYPEEXPR_TO_TYPE.put("float", DataTypes.FloatType);
//		TYPEEXPR_TO_TYPE.put("double", DataTypes.DoubleType);
//		TYPEEXPR_TO_TYPE.put("long", DataTypes.LongType);
//		TYPEEXPR_TO_TYPE.put("binary", DataTypes.BinaryType);
//		TYPEEXPR_TO_TYPE.put("short", DataTypes.ShortType);
//		TYPEEXPR_TO_TYPE.put("byte", DataTypes.ByteType);
//		TYPEEXPR_TO_TYPE.put("boolean", DataTypes.BooleanType);
//		TYPEEXPR_TO_TYPE.put("timestamp", DataTypes.TimestampType);
//		TYPEEXPR_TO_TYPE.put("date", DataTypes.DateType);
//		TYPEEXPR_TO_TYPE.put("float", DataTypes.FloatType);
//
//		SERIALIZERS.put(DataTypes.StringType, str -> UTF8String.fromString(str.toString()));
////		SERIALIZERS.put(DataTypes.StringType, str -> str.toString());
//		SERIALIZERS.put(DataTypes.IntegerType, DataUtils::asInt);
//		SERIALIZERS.put(DataTypes.LongType, DataUtils::asLong);
//		SERIALIZERS.put(DataTypes.ShortType, DataUtils::asShort);
//		SERIALIZERS.put(DataTypes.ByteType, DataUtils::asByte);
//		SERIALIZERS.put(DataTypes.DoubleType, DataUtils::asDouble);
//		SERIALIZERS.put(DataTypes.FloatType, DataUtils::asFloat);
//		SERIALIZERS.put(DataTypes.BooleanType, DataUtils::asBoolean);
//		SERIALIZERS.put(DataTypes.DateType, obj -> {
//			if ( obj instanceof java.sql.Date ) {
//				long millis = ((java.sql.Date)obj).getTime();
//				return (int)(millis / (1000 * 60 * 60 * 24)) + 1;
//			}
//			else if ( obj instanceof java.util.Date ) {
//				long millis = ((java.util.Date)obj).getTime();
//				return (int)(millis / (1000 * 60 * 60 * 24)) + 1;
//			}
//			else if ( obj == null ) {
//				return null;
//			}
//			else {
//				throw new IllegalArgumentException("invalid Date data: obj=" + obj);
//			}
//		});
//		
//		DESERIALIZERS.put(DataTypes.createArrayType(DataTypes.StringType), TypeUtils::unwrapStringArray);
//		DESERIALIZERS.put(DataTypes.createArrayType(DataTypes.DoubleType), TypeUtils::unwrapDoubleArray);
//		DESERIALIZERS.put(DataTypes.createArrayType(DataTypes.LongType), TypeUtils::unwrapLongArray);
	};
//	
//	public static SpatialDatasetInfo parseSpatialDatasetInfo(Map<String,Object> yaml) {
//		DatasetType type = DatasetType.fromString((String)yaml.get("type"));
//		GeometryColumnInfo gcInfo = GeometryColumnInfo.fromString((String)yaml.get("geometry"));
//		
//		Map<String,Object> schemaYaml = (Map<String,Object>)yaml.get("schema");
//		StructField[] fields = FStream.from(schemaYaml)
//									.map(kv -> {
//										DataType dataType = parseTypeExpr((String)kv.value())._1;
//										return DataTypes.createStructField(kv.key(), dataType, true);
//									})
//									.toArray(StructField.class);
//		StructType schema = DataTypes.createStructType(fields);
//		List<Object> qidList = (List<Object>)yaml.get("quad_ids");
//		if ( qidList != null ) {
//			Long[] qids = FStream.from(qidList).map(DataUtils::asLong).toArray(Long.class);
//			return new SpatialDatasetInfo(type, gcInfo, schema, qids);
//		}
//		else {
//			return new SpatialDatasetInfo(type, gcInfo, schema, null);
//		}
//	}
	
	public static DataType toDataType(Class<?> cls) {
		if ( Geometry.class.isAssignableFrom(cls) ) {
			return GeometryType.DATA_TYPE;
		}
		else if ( Envelope.class.isAssignableFrom(cls) ) {
			return EnvelopeValue.DATA_TYPE;
		}
		else if ( String[].class.equals(cls) ) {
			return DataTypes.createArrayType(DataTypes.StringType);
		}
		else if ( double[].class.equals(cls) ) {
			return DataTypes.createArrayType(DataTypes.DoubleType);
		}
		else {
			DataType dtype = CLASS_TO_TYPES.get(cls);
			if ( dtype == null ) {
				throw new IllegalArgumentException("unknown Java class: " + cls);
			}
			
			return dtype;
		}
	}

//	public static String toDataTypeString(DataType dtype) {
//		if ( dtype instanceof ArrayType ) {
//			ArrayType arrType = (ArrayType)dtype;
//			String nullExpr = arrType.containsNull() ? "nullable" : "non-null";
//			return String.format("array<%s,%s>", toDataTypeString(arrType.elementType()), nullExpr);
//		}
//		else {
//			String name = TYPE_NAMES.get(dtype);
//			if ( name != null ) {
//				return name;
//			}
//			
//			throw new IllegalArgumentException("unknown data-type: " + dtype);
//		}
//	}
//
//	private static final Pattern SRID_PATTERN = Pattern.compile("\\(\\s*([0-9]+)?\\s*\\)");
//	private static final Pattern ARRAY_SUBTYPE_PATTERN = Pattern.compile("\\<\\s*(\\S+)\\s*(,\\S+)\\>");
//	public static Tuple<DataType,Map<String,Object>> parseTypeExpr(String typeExpr) {
//		typeExpr = typeExpr.trim().toLowerCase();
//		
//		if ( typeExpr.startsWith("geometry") ) {
//			String subExpr = typeExpr.substring("geometry".length());
//			Matcher matcher = SRID_PATTERN.matcher(subExpr);
//			if ( matcher.find() ) {
//				if ( matcher.groupCount() == 1 ) {
//					int srid = Integer.parseInt(matcher.group(1));
//					Map<String,Object> meta = Maps.newHashMap();
//					meta.put("jarvey.type", "geometry");
//					meta.put("srid", srid);
//					return Tuple.of(GeometryValue.DATA_TYPE, meta);
//				}
//				else {
//					throw new IllegalArgumentException(String.format("invalid geometry type: '%s'", typeExpr));
//				}
//			}
//			else {
//				Map<String,Object> meta = Maps.newHashMap();
//				meta.put("srid", 0);
//				return Tuple.of(GeometryValue.DATA_TYPE, meta) ;
//			}
//		}
//		else if ( typeExpr.startsWith("envelope") ) {
//			Map<String,Object> meta = Maps.newHashMap();
//			meta.put("jarvey.type", "envelope");
//			return Tuple.of(EnvelopeValue.DATA_TYPE, meta);
//		}
//		else if ( typeExpr.startsWith("array") ) {
//			String subTypeName = typeExpr.substring("array".length());
//			Matcher matcher = ARRAY_SUBTYPE_PATTERN.matcher(subTypeName);
//			if ( !matcher.find() ) {
//				throw new IllegalArgumentException(String.format("invalid array type: '%s'", typeExpr));
//			}
//			
//			int groupCnt = matcher.groupCount();
//			if ( groupCnt >= 1 ) {
//				subTypeName = matcher.group(1);
//				DataType elmType = parseTypeExpr(subTypeName)._1;
//				return Tuple.of(DataTypes.createArrayType(elmType, true), Collections.emptyMap());
//			}
//			throw new IllegalArgumentException(String.format("invalid array type: '%s'", typeExpr));
//		}
//		
//		DataType dtype = TYPEEXPR_TO_TYPE.get(typeExpr);
//		if ( dtype != null ) {
//			return Tuple.of(dtype, Collections.emptyMap());
//		}
//		
//		throw new IllegalArgumentException(String.format("invalid type: '%s'", typeExpr));
//	}
//	
//	public static Map<String,Object> toYaml(SpatialDatasetInfo info) {
//		Map<String,Object> root = Maps.newLinkedHashMap();
//		root.put("type", info.getType().toString());
//		root.put("geometry", info.getDefaultGeometryColumnInfo().toString());
//		root.put("schema", toYaml(info.getSchema()));
//		if ( info.getQuadIds() != null ) {
//			root.put("quad_ids", info.getQuadIds());
//		}
//		
//		return root;
//	}
//	
//	private static Map<String,Object> toYaml(StructType schema) {
//		Map<String,Object> fieldMap = FStream.of(schema.fields())
//											.foldLeft(Maps.newLinkedHashMap(), (map, field) -> {
//												map.put(field.name(), TypeUtils.toDataTypeString(field.dataType()));
//												return map;
//											});
//		return fieldMap;
//	}
	
//	public static <T> void copyArray(WrappedArray<T> array, Collection<T> output) {
//		if ( array != null ) {
//			for ( int i =0; i < array.length(); ++i ) {
//				output.add(array.apply(i));
//			}
//		}
//	}
}
