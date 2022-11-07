package jarvey.type;

import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import jarvey.JarveyRuntimeException;

import utils.geo.util.GeoClientUtils;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryType extends JarveyDataType {
	private static final long serialVersionUID = 1L;
	
	public static final DataType DATA_TYPE = DataTypes.BinaryType;
	public static final StructType ROW_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("wkb", DATA_TYPE, true),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(ROW_TYPE);
	
	private final Geometries m_geomType;
	private final int m_srid;
	
	public static final GeometryType of(Geometries geomType, int srid) {
		return new GeometryType(geomType, srid);
	}
	
	private GeometryType(Geometries geomType, int srid) {
		super(DATA_TYPE);

		m_geomType = geomType;
		m_srid = srid;
	}
	
	public Geometries getGeometries() {
		return m_geomType;
	}
	
	public int getSrid() {
		return m_srid;
	}
	
	public GeometryType newGeometryType(int srid) {
		return of(m_geomType, srid);
	}
	
	public Geometry newEmptyInstance() {
		return GeoClientUtils.emptyGeometry(m_geomType);
	}

	@Override
	public Class<? extends Geometry> getJavaClass() {
		return GEOMETRY_TYPES.get(m_geomType);
	}
	
	public static GeometryType fromJavaClass(Class<?> cls, int srid) {
		if ( MultiPolygon.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.MULTIPOLYGON, srid);
		}
		else if ( Point.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.POINT, srid);
		}
		else if ( Polygon.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.POLYGON, srid);
		}
		else if ( LineString.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.LINESTRING, srid);
		}
		else if ( GeometryCollection.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.GEOMETRYCOLLECTION, srid);
		}
		else if ( MultiPoint.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.MULTIPOINT, srid);
		}
		else if ( MultiLineString.class.isAssignableFrom(cls) ) {
			return GeometryType.of(Geometries.MULTILINESTRING, srid);
		}
		else {
			throw new IllegalArgumentException("unknown Geometry Java class: " + cls);
		}
	}
	
	@Override
	public byte[] serialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof Geometry ) {
			return GeoClientUtils.toWKB((Geometry)value);
		}
		else {
			throw new JarveyRuntimeException("invalid Geometry: " + value);
		}
	}

	@Override
	public Geometry deserialize(Object value) {
		if ( value == null ) {
			return null;
		}
		else if ( value instanceof byte[] ) {
			try {
				return GeoClientUtils.fromWKB((byte[])value);
			}
			catch ( ParseException e ) {
				throw new JarveyRuntimeException("fails to parse WKB, cause=" + e);
			}
		}
		else {
			throw new JarveyRuntimeException("invalid serialized value for Geometry: " + value);
		}
	}
	
	public static GeometryType fromString(String geomTypeName, int srid) {
		return of(TYPE_NAMES.inverse().get(geomTypeName), srid);
	}
	
	@Override
	public String toString() {
		return String.format("%s(%d)", TYPE_NAMES.get(m_geomType), m_srid);
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof GeometryType) ) {
			return false;
		}
		
		GeometryType other = (GeometryType)obj;
		return m_srid == other.m_srid;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(getSparkType(), m_srid);
	}
	
	public static final Map<Geometries, Class<? extends Geometry>> GEOMETRY_TYPES = Maps.newHashMap();
	public static final BiMap<Geometries, String> TYPE_NAMES = HashBiMap.create();
	static {
		GEOMETRY_TYPES.put(Geometries.GEOMETRY, Geometry.class);
		GEOMETRY_TYPES.put(Geometries.POINT, Point.class);
		GEOMETRY_TYPES.put(Geometries.MULTIPOINT, MultiPoint.class);
		GEOMETRY_TYPES.put(Geometries.LINESTRING, LineString.class);
		GEOMETRY_TYPES.put(Geometries.MULTILINESTRING, MultiLineString.class);
		GEOMETRY_TYPES.put(Geometries.POLYGON, Polygon.class);
		GEOMETRY_TYPES.put(Geometries.MULTIPOLYGON, MultiPolygon.class);
		GEOMETRY_TYPES.put(Geometries.GEOMETRYCOLLECTION, GeometryCollection.class);
		
		TYPE_NAMES.put(Geometries.GEOMETRY, "Geometry");
		TYPE_NAMES.put(Geometries.POINT, "Point");
		TYPE_NAMES.put(Geometries.MULTIPOINT, "MultiPoint");
		TYPE_NAMES.put(Geometries.LINESTRING, "LineString");
		TYPE_NAMES.put(Geometries.MULTILINESTRING, "MultiLineString");
		TYPE_NAMES.put(Geometries.POLYGON, "Polygon");
		TYPE_NAMES.put(Geometries.MULTIPOLYGON, "MultiPolygon");
		TYPE_NAMES.put(Geometries.GEOMETRYCOLLECTION, "GeometryCollection");
	};
}
