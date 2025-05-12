package jarvey.type;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Maps;

import utils.Tuple;

import jarvey.support.typeexpr.JarveyTypeParser;
import jarvey.type.temporal.TemporalPointType;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataTypes {
	private JarveyDataTypes() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static final StringType String_Type = StringType.get();
	public static final LongType Long_Type = LongType.get();
	public static final IntegerType Integer_Type = IntegerType.get();
	public static final ShortType Short_Type = ShortType.get();
	public static final ByteType Byte_Type = ByteType.get();
	public static final DoubleType Double_Type = DoubleType.get();
	public static final FloatType Float_Type = FloatType.get();
	public static final BinaryType Binary_Type = BinaryType.get();
	public static final BooleanType Boolean_Type = BooleanType.get();
	public static final DateType Date_Type = DateType.get();
	public static final TimestampType Timestamp_Type = TimestampType.get();
	public static final CalendarIntervalType CalendarInterval_Type = CalendarIntervalType.get();

	public static final EnvelopeType Envelope_Type = EnvelopeType.get();
	public static final GridCellType GridCell_Type = GridCellType.get();
	public static final GeometryType Geometry_Type = GeometryType.of(Geometries.GEOMETRY, 0);
	public static final GeometryType PointType = GeometryType.of(Geometries.POINT, 0);
	public static final GeometryType MultiPointType = GeometryType.of(Geometries.MULTIPOINT, 0);
	public static final GeometryType LineStringType = GeometryType.of(Geometries.LINESTRING, 0);
	public static final GeometryType MultiLineStringType = GeometryType.of(Geometries.MULTILINESTRING, 0);
	public static final GeometryType PolygonType = GeometryType.of(Geometries.POLYGON, 0);
	public static final GeometryType MultiPolygonType = GeometryType.of(Geometries.MULTIPOLYGON, 0);
	public static final GeometryType GeometryCollectionType = GeometryType.of(Geometries.GEOMETRYCOLLECTION, 0);
	
	public static final LongArrayType LongArray_Type = LongArrayType.of(true);
	public static final LongArrayType LongArray_TypeN = LongArrayType.of(false);
	
	public static final JarveyArrayType DoubleArrayType = JarveyArrayType.of(Double_Type, true);
	public static final JarveyArrayType DoubleArrayTypeN = JarveyArrayType.of(Double_Type, false);
	
	public static final VectorType Vector_Type = VectorType.get();
	
	public static final TemporalPointType Temporal_Point_Type = TemporalPointType.get();

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
		
		JarveyDataType jtype = JAVA_TO_JARVEY.get(cls);
		if ( jtype != null ) {
			return jtype;
		}
		
		Class<?> elmCls = cls.getComponentType();
		if ( elmCls != null ) {
			JarveyDataType elmJType = fromJavaClass(elmCls);
			return JarveyArrayType.of(elmJType, true);
		}
		
		throw new IllegalArgumentException("unknown Java class: " + cls);
	}
	
	public static JarveyDataType fromSparkType(DataType sparkType) {
		JarveyDataType jtype = SPARK_TO_JARVEY.get(sparkType);
		if ( jtype != null ) {
			return jtype;
		}
		
		if ( sparkType instanceof org.apache.spark.sql.types.ArrayType ) {
			org.apache.spark.sql.types.ArrayType arrType = (org.apache.spark.sql.types.ArrayType)sparkType;
			DataType elmType = arrType.elementType();
			JarveyDataType elmJaveyType = fromSparkType(elmType);
			
			return new JarveyArrayType(elmJaveyType, true);
		}
		else if ( sparkType instanceof StructType ) {
			if ( sparkType.equals(TemporalPointType.SCHEMA) ) {
				return JarveyDataTypes.Temporal_Point_Type;
			}
		}
		else if ( sparkType instanceof VectorUDT ) {
			return JarveyDataTypes.Vector_Type;
		}
		
		throw new IllegalArgumentException("unknown Spark DataType: " + sparkType);
	}
	
	public static Tuple<JarveyDataType, Dataset<Row>> unwrap(StructField field, Dataset<Row> df) {
		String name = field.name();
		StructType rootType = (StructType)field.dataType();
		
		DataType fieldType = rootType.fields()[0].dataType();
		if ( fieldType instanceof org.apache.spark.sql.types.ByteType ) {
			JarveyDataType type = JarveyDataTypes.Geometry_Type;
			String expr = String.format("%s.wkb", name);
			df = df.withColumn(name, df.col(expr));
			
			return Tuple.of(type, df);
		}
		else if ( fieldType instanceof ArrayType ) {
			DataType elmType = ((ArrayType)fieldType).elementType();
			if ( elmType instanceof org.apache.spark.sql.types.DoubleType ) {
				JarveyDataType type = JarveyDataTypes.Envelope_Type;
				String expr = String.format("%s.coordinates", name);
				df = df.withColumn(name, df.col(expr));
				
				return Tuple.of(type, df);
			}
			else if ( elmType instanceof org.apache.spark.sql.types.IntegerType ) {
				JarveyDataType type = JarveyDataTypes.GridCell_Type;
				String expr = String.format("%s.coordinates", name);
				df = df.withColumn(name, df.col(expr));
				
				return Tuple.of(type, df);
			}
		}
		
		return null;
	}

	public static final Map<Class<?>, JarveyDataType> JAVA_TO_JARVEY = Maps.newHashMap();
	public static final Map<DataType, JarveyDataType> SPARK_TO_JARVEY = Maps.newHashMap();
	static {
		JAVA_TO_JARVEY.put(String.class, JarveyDataTypes.String_Type);
		JAVA_TO_JARVEY.put(Long.class, JarveyDataTypes.Long_Type);
		JAVA_TO_JARVEY.put(Integer.class, JarveyDataTypes.Integer_Type);
		JAVA_TO_JARVEY.put(Short.class, JarveyDataTypes.Short_Type);
		JAVA_TO_JARVEY.put(Byte.class, JarveyDataTypes.Byte_Type);
		JAVA_TO_JARVEY.put(Double.class, JarveyDataTypes.Double_Type);
		JAVA_TO_JARVEY.put(Float.class, JarveyDataTypes.Float_Type);
		JAVA_TO_JARVEY.put(byte[].class, JarveyDataTypes.Binary_Type);
		JAVA_TO_JARVEY.put(Boolean.class, JarveyDataTypes.Boolean_Type);
		JAVA_TO_JARVEY.put(Date.class, JarveyDataTypes.Date_Type);
		JAVA_TO_JARVEY.put(java.util.Date.class, JarveyDataTypes.Date_Type);
		JAVA_TO_JARVEY.put(java.sql.Date.class, JarveyDataTypes.Date_Type);
		JAVA_TO_JARVEY.put(Timestamp.class, JarveyDataTypes.Timestamp_Type);
		
		JAVA_TO_JARVEY.put(Geometry.class, JarveyDataTypes.Geometry_Type);
		JAVA_TO_JARVEY.put(Envelope.class, JarveyDataTypes.Envelope_Type);
		JAVA_TO_JARVEY.put(GridCell.class, JarveyDataTypes.GridCell_Type);
		
		JAVA_TO_JARVEY.put(long[].class, JarveyDataTypes.LongArray_Type);
		JAVA_TO_JARVEY.put(Long[].class, JarveyDataTypes.LongArray_Type);
		
		
		SPARK_TO_JARVEY.put(DataTypes.StringType, JarveyDataTypes.String_Type);
		SPARK_TO_JARVEY.put(DataTypes.LongType, JarveyDataTypes.Long_Type);
		SPARK_TO_JARVEY.put(DataTypes.IntegerType, JarveyDataTypes.Integer_Type);
		SPARK_TO_JARVEY.put(DataTypes.ShortType, JarveyDataTypes.Short_Type);
		SPARK_TO_JARVEY.put(DataTypes.ByteType, JarveyDataTypes.Byte_Type);
		SPARK_TO_JARVEY.put(DataTypes.DoubleType, JarveyDataTypes.Double_Type);
		SPARK_TO_JARVEY.put(DataTypes.FloatType, JarveyDataTypes.Float_Type);
		SPARK_TO_JARVEY.put(DataTypes.BinaryType, JarveyDataTypes.Binary_Type);
		SPARK_TO_JARVEY.put(DataTypes.BooleanType, JarveyDataTypes.Boolean_Type);
		SPARK_TO_JARVEY.put(DataTypes.DateType, JarveyDataTypes.Date_Type);
		SPARK_TO_JARVEY.put(DataTypes.TimestampType, JarveyDataTypes.Timestamp_Type);
	};
}
