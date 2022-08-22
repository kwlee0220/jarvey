package jarvey.type;

import org.apache.spark.sql.types.DataTypes;
import org.geotools.geometry.jts.Geometries;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataTypes {
	private JarveyDataTypes() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static final RegularType StringType = RegularType.of(DataTypes.StringType);
	public static final RegularType LongType = RegularType.of(DataTypes.LongType);
	public static final RegularType IntegerType = RegularType.of(DataTypes.IntegerType);
	public static final RegularType ShortType = RegularType.of(DataTypes.ShortType);
	public static final RegularType ByteType = RegularType.of(DataTypes.ByteType);
	public static final RegularType DoubleType = RegularType.of(DataTypes.DoubleType);
	public static final RegularType FloatType = RegularType.of(DataTypes.FloatType);
	public static final RegularType BinaryType = RegularType.of(DataTypes.BinaryType);
	public static final RegularType BooleanType = RegularType.of(DataTypes.BooleanType);
	public static final RegularType DateType = RegularType.of(DataTypes.DateType);
	public static final RegularType TimestampType = RegularType.of(DataTypes.TimestampType);
	public static final RegularType CalendarIntervalType = RegularType.of(DataTypes.CalendarIntervalType);

	public static final EnvelopeType Envelope_Type = EnvelopeType.get();
	public static final GeometryType Geometry_Type = GeometryType.of(Geometries.GEOMETRY, 0);
	public static final GeometryType PointType = GeometryType.of(Geometries.POINT, 0);
	public static final GeometryType MultiPointType = GeometryType.of(Geometries.MULTIPOINT, 0);
	public static final GeometryType LineStringType = GeometryType.of(Geometries.LINESTRING, 0);
	public static final GeometryType MultiLineStringType = GeometryType.of(Geometries.MULTILINESTRING, 0);
	public static final GeometryType PolygonType = GeometryType.of(Geometries.POLYGON, 0);
	public static final GeometryType MultiPolygonType = GeometryType.of(Geometries.MULTIPOLYGON, 0);
	public static final GeometryType GeometryCollectionType = GeometryType.of(Geometries.GEOMETRYCOLLECTION, 0);
	
	public static final ArrayType LongArrayType = ArrayType.of(LongType, true);
	public static final ArrayType LongArrayTypeN = ArrayType.of(LongType, false);
	
	public static final ArrayType DoubleArrayType = ArrayType.of(DoubleType, true);
	public static final ArrayType DoubleArrayTypeN = ArrayType.of(DoubleType, false);
}
