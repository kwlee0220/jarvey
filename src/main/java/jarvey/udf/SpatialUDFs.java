package jarvey.udf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

import jarvey.JarveyRuntimeException;
import jarvey.support.MapTile;
import jarvey.type.DataUtils;
import jarvey.type.EnvelopeBean;
import jarvey.type.EnvelopeType;
import jarvey.type.GeometryArrayBean;
import jarvey.type.GeometryBean;
import jarvey.type.GeometryType;
import jarvey.type.JarveyArrayType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.temporal.MakeLineUDAF;

import scala.collection.mutable.WrappedArray;
import utils.func.Tuple;
import utils.geo.util.CoordinateTransform;
import utils.geo.util.GeoClientUtils;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialUDFs {
	private static final EnvelopeType SERDE_ENVELOPE = JarveyDataTypes.Envelope_Type;
	
	private SpatialUDFs() {
		throw new AssertionError("Should not be called: class=" + SpatialUDFs.class);
	}
	
	static interface GeometryFactory<T> extends Serializable {
		public Geometry create(T arg1);
	}
	private static final <T> UDF1<T, byte[]> adapt(GeometryFactory<T> fact) {
		return (v1) -> GeometryBean.serialize(fact.create(v1));
	};
	
	static interface GeometryOperator extends Serializable {
		public Geometry apply(Geometry geom);
	}
	private static final UDF1<byte[], byte[]> adapt(GeometryOperator optor) {
		return wkb -> {
			Geometry in = GeometryBean.deserialize(wkb);
			return GeometryBean.serialize(optor.apply(in));
		};
	};
	
	static interface Arg1GeometryOperator<T> extends Serializable {
		public Geometry apply(Geometry geom, T arg1);
	}
	private static final <T> UDF2<byte[], T, byte[]> adapt(Arg1GeometryOperator<T> optor) {
		return (wkb, arg) -> {
			Geometry in = GeometryBean.deserialize(wkb);
			Geometry out = optor.apply(in, arg);
			return GeometryBean.serialize(out);
		};
	};
	
	static interface BinaryGeometryOperator extends Serializable {
		public Geometry apply(Geometry geom1, Geometry geom2);
	}
	private static final UDF2<byte[],byte[],byte[]> adapt(BinaryGeometryOperator optor) {
		return (wkb1, wkb2) -> {
			Geometry geom1 = GeometryBean.deserialize(wkb1);
			Geometry geom2 = GeometryBean.deserialize(wkb2);
			
			Geometry out = optor.apply(geom1, geom2);
			return GeometryBean.serialize(out);
		};
	};
	
	static interface GeometryFunction<T> extends Serializable {
		public T apply(Geometry geom);
	}
	private static final <T> UDF1<byte[], T> adapt(GeometryFunction<T> func) {
		return (wkb) -> {
			Geometry geom = GeometryBean.deserialize(wkb);
			return func.apply(geom);
		};
	};
	
	static interface Arg1GeometryFunction<IN,OUT> extends Serializable {
		public OUT apply(Geometry geom, IN arg1);
	}
	private static final <IN,OUT> UDF2<byte[], IN, OUT> adapt(Arg1GeometryFunction<IN,OUT> func) {
		return (wkb, arg) -> {
			Geometry geom = GeometryBean.deserialize(wkb);
			return func.apply(geom, arg);
		};
	};
	
	static interface BinaryGeometryFunction<T> extends Serializable {
		public T apply(Geometry geom1, Geometry geom2);
	}
	private static final <T> UDF2<byte[], byte[], T> adapt(BinaryGeometryFunction<T> func) {
		return (wkb1, wkb2) -> {
			Geometry geom1 = GeometryBean.deserialize(wkb1);
			Geometry geom2 = GeometryBean.deserialize(wkb2);
			return func.apply(geom1, geom2);
		};
	};
	
	static interface Arg1BinaryGeometryFunction<IN,OUT> extends Serializable {
		public OUT apply(Geometry geom1, Geometry geom2, IN arg1);
	}
	private static final <IN,T> UDF3<byte[], byte[], IN, T> adapt(Arg1BinaryGeometryFunction<IN,T> func) {
		return (wkb1, wkb2, arg1) -> {
			Geometry geom1 = GeometryBean.deserialize(wkb1);
			Geometry geom2 = GeometryBean.deserialize(wkb2);
			return func.apply(geom1, geom2, arg1);
		};
	};
	
	static interface Arg1BoxOperator<T> extends Serializable {
		public Envelope apply(Envelope envl, T arg1);
	}
	private static final <T> UDF2<WrappedArray<Double>, T, Double[]> adapt(Arg1BoxOperator<T> optor) {
		return (coords, arg) -> {
			Envelope envl = SERDE_ENVELOPE.deserialize(coords);
			Envelope out = optor.apply(envl, arg);
			return SERDE_ENVELOPE.serialize(out);
		};
	};
	
	static interface Arg2BoxOperator<T1, T2> extends Serializable {
		public Envelope apply(Envelope envl, T1 arg1, T2 arg2);
	}
	private static final <T1, T2> UDF3<WrappedArray<Double>, T1, T2, Double[]>
	adapt(Arg2BoxOperator<T1,T2> optor) {
		return (coords, arg1, arg2) -> {
			Envelope envl = SERDE_ENVELOPE.deserialize(coords);
			Envelope out = optor.apply(envl, arg1, arg2);
			return SERDE_ENVELOPE.serialize(out);
		};
	};
	
	static interface BoxFunction<T> extends Serializable {
		public T apply(Envelope envl);
	}
	private static final <T> UDF1<WrappedArray<Double>, T> adapt(BoxFunction<T> optor) {
		return (coords) -> {
			Envelope envl = EnvelopeBean.deserialize(coords);
			return optor.apply(envl);
		};
	};
	
	public static void registerUdf(UDFRegistration registry) {
		registry.register("ST_Area", adapt(ST_Area), DataTypes.DoubleType);
		registry.register("ST_AsBinary", ST_AsBinary, DataTypes.BinaryType);
		registry.register("ST_AsText", adapt(ST_AsText), DataTypes.StringType);
		registry.register("ST_Boundary", adapt(ST_Boundary), GeometryType.DATA_TYPE);
		registry.register("ST_Buffer", adapt(ST_Buffer), GeometryType.DATA_TYPE);
		registry.register("ST_Centroid", adapt(ST_Centroid), GeometryType.DATA_TYPE);
		registry.register("ST_ReducePrecision", adapt(ST_ReducePrecision), GeometryType.DATA_TYPE);
		registry.register("ST_Contains", adapt(ST_Contains), DataTypes.BooleanType);
		registry.register("ST_ConvexHull", adapt(ST_ConvexHull), GeometryType.DATA_TYPE);
		registry.register("ST_CoordDim", adapt(ST_CoordDim), DataTypes.IntegerType);
		registry.register("ST_Crosses", adapt(ST_Crosses), DataTypes.BooleanType);
//		registry.register("ST_CurveToLine", ST_CurveToLine, DataTypes.IntegerType);
		registry.register("ST_Difference", adapt(ST_Difference), GeometryType.DATA_TYPE);
		registry.register("ST_Dimension", adapt(ST_Dimension), DataTypes.IntegerType);
		registry.register("ST_Disjoint", adapt(ST_Disjoint), DataTypes.IntegerType);
		registry.register("ST_Distance", adapt(ST_Distance), DataTypes.DoubleType);
		registry.register("ST_EndPoint", adapt(ST_EndPoint), GeometryType.DATA_TYPE);
		registry.register("ST_Envelope", adapt(ST_Envelope), GeometryType.DATA_TYPE);
		registry.register("ST_Equals", adapt(ST_Equals), DataTypes.BooleanType);
		registry.register("ST_ExteriorRing", adapt(ST_ExteriorRing), GeometryType.DATA_TYPE);
		registry.register("ST_GeomCollFromText", adapt(ST_GeomCollFromText), GeometryType.DATA_TYPE);
		registry.register("ST_GeomCollFromWKB", adapt(ST_GeomCollFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_GeomFromText", adapt(ST_GeomFromText), GeometryType.DATA_TYPE);
		registry.register("ST_GeomFromWKB", adapt(ST_GeomFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_GeometryFromText", adapt(ST_GeomFromText), GeometryType.DATA_TYPE);
		registry.register("ST_GeometryN", adapt(ST_GeometryN), GeometryType.DATA_TYPE);
		registry.register("ST_GeometryType", adapt(ST_GeometryType), DataTypes.StringType);
		registry.register("ST_InteriorRingN", adapt(ST_InteriorRingN), GeometryType.DATA_TYPE);
		registry.register("ST_Intersection", adapt(ST_Intersection), GeometryType.DATA_TYPE);
		registry.register("ST_Intersects", adapt(ST_Intersects), DataTypes.BooleanType);
		registry.register("ST_IntersectsGeom", adapt(ST_IntersectsGeom), DataTypes.BooleanType);

		// kwlee
		registry.register("TEST_Intersects", adapt(TEST_Intersects), DataTypes.BooleanType);
		
		registry.register("ST_IsClosed", adapt(ST_IsClosed), DataTypes.BooleanType);
		registry.register("ST_IsRing", adapt(ST_IsRing), DataTypes.BooleanType);
		registry.register("ST_IsSimple", adapt(ST_IsSimple), DataTypes.BooleanType);
		registry.register("ST_IsValid", adapt(ST_IsValid), DataTypes.BooleanType);
		registry.register("ST_IsValidEnvelope", adapt(ST_IsValidEnvelope), DataTypes.BooleanType);
		registry.register("ST_Length", adapt(ST_Length), DataTypes.DoubleType);
		registry.register("ST_LineFromText", adapt(ST_LineFromText), GeometryType.DATA_TYPE);
		registry.register("ST_LineFromWKB", adapt(ST_LineFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_LineStringFromWKB", adapt(ST_LineFromWKB), GeometryType.DATA_TYPE);
//		registry.register("ST_M", ST_M, GeometryRow.DATA_TYPE);
		registry.register("ST_MakeLine", adapt(ST_MakeLine), GeometryType.DATA_TYPE);
		registry.register("ST_MakeLineA", ST_MakeLineA, GeometryType.DATA_TYPE);
		registry.register("ST_MLineFromText", adapt(ST_MLineFromText), GeometryType.DATA_TYPE);
		registry.register("ST_MLineFromWKB", adapt(ST_MLineFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_MPointFromText", adapt(ST_MPointFromText), GeometryType.DATA_TYPE);
		registry.register("ST_MPointFromWKB", adapt(ST_MPointFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_MPolyFromText", adapt(ST_MPolyFromText), GeometryType.DATA_TYPE);
		registry.register("ST_MPolytFromWKB", adapt(ST_MPolyFromWKB), GeometryType.DATA_TYPE);

		registry.register("ST_NumGeometries", adapt(ST_NumGeometries), DataTypes.IntegerType);
		registry.register("ST_NumInteriorRings", adapt(ST_NumInteriorRings), DataTypes.IntegerType);
		registry.register("ST_NumPoints", adapt(ST_NumPoints), DataTypes.IntegerType);
		registry.register("ST_OrderingEquals", adapt(ST_OrderingEquals), DataTypes.BooleanType);
		registry.register("ST_Overlaps", adapt(ST_Overlaps), DataTypes.BooleanType);
//		registry.register("ST_Perimeter", adapt(ST_Perimeter), DataTypes.DoubleType);
		registry.register("ST_Point", ST_Point, GeometryType.DATA_TYPE);
		registry.register("ST_PointFromText", adapt(ST_PointFromText), GeometryType.DATA_TYPE);
		registry.register("ST_PointFromWKB", adapt(ST_PointFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_PointN", adapt(ST_PointN), GeometryType.DATA_TYPE);
//		registry.register("ST_PointOnSurface", ST_PointOnSurface, DataTypes.IntegerType);
//		registry.register("ST_Polygon", ST_Polygon, DataTypes.IntegerType);
		registry.register("ST_PolygonFromText", adapt(ST_PolygonFromText), GeometryType.DATA_TYPE);
		registry.register("ST_PolygonFromWKB", adapt(ST_PolygonFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_Relate", adapt(ST_Relate), DataTypes.BooleanType);
		registry.register("ST_RelatePattern", adapt(ST_RelatePattern), GeometryType.DATA_TYPE);
		registry.register("ST_StartPoint", adapt(ST_StartPoint), GeometryType.DATA_TYPE);
		registry.register("ST_SymDifference", adapt(ST_SymDifference), GeometryType.DATA_TYPE);
		registry.register("ST_Touches", adapt(ST_Touches), GeometryType.DATA_TYPE);
		registry.register("ST_Transform", ST_Transform, GeometryType.DATA_TYPE);
//		registry.register("ST_Union", adapt(ST_Union), GeometryRow.DATA_TYPE);
		registry.register("ST_WKBToSQL", adapt(ST_GeomFromWKB), GeometryType.DATA_TYPE);
		registry.register("ST_WKTToSQL", adapt(ST_GeomFromText), GeometryType.DATA_TYPE);
		registry.register("ST_Within", adapt(ST_Within), DataTypes.BooleanType);
		registry.register("ST_X", adapt(ST_X), DataTypes.DoubleType);
		registry.register("ST_Y", adapt(ST_Y), DataTypes.DoubleType);
//		registry.register("ST_Z", adapt(ST_Z), DataTypes.DoubleType);
		
		registry.register("Box2D", adapt(Box2D), EnvelopeBean.DATA_TYPE);
		registry.register("ST_ExpandBox1", adapt(ST_ExpandBox1), EnvelopeBean.DATA_TYPE);
		registry.register("ST_ExpandBox2", adapt(ST_ExpandBox2), EnvelopeBean.DATA_TYPE);
		registry.register("ST_TransformBox", adapt(ST_TransformBox), EnvelopeBean.DATA_TYPE);
		registry.register("ST_BoxIntersectsBox", ST_BoxIntersectsBox, DataTypes.BooleanType);
		registry.register("ST_GeomIntersectsBox", ST_GeomIntersectsBox, DataTypes.BooleanType);
		registry.register("ST_XMin", adapt(ST_XMin), DataTypes.DoubleType);
		registry.register("ST_XMax", adapt(ST_XMax), DataTypes.DoubleType);
		registry.register("ST_YMin", adapt(ST_YMin), DataTypes.DoubleType);
		registry.register("ST_YMax", adapt(ST_YMax), DataTypes.DoubleType);

//		registry.register("ST_AsGeoJSON", ST_AsGeoJSON, DataTypes.StringType);
//		registry.register("ST_GeomFromGeoJSON", ST_GeomFromGeoJSON, GeometryRow.DATA_TYPE);

		// user-defined aggregation functions
//		registry.register("ST_Accum", functions.udaf(new AccumGeometryUDAF(), AccumGeometryUDAF.INPUT_ENCODER));
		registry.register("ST_Collect", functions.udaf(new CollectUDAF(), CollectUDAF.INPUT_ENCODER));
		registry.register("ST_Union", functions.udaf(new UnionUDAF(), UnionUDAF.INPUT_ENCODER));
		registry.register("ST_Extent", functions.udaf(new ExtentUDAF(), ExtentUDAF.INPUT_ENCODER));
		registry.register("JV_SummarizeSpatialInfo",
						functions.udaf(new SummarizeSpatialInfoUDAF(), SummarizeSpatialInfoUDAF.INPUT_ENCODER));
		registry.register("ST_AggMakeLine", functions.udaf(new MakeLineUDAF(), MakeLineUDAF.INPUT_ENCODER));
//		registry.register("ST_MemUnion", new UnionGeomUDAF());
//		registry.register("ST_Polygonize", new UnionGeomUDAF());
		
		registry.register("JV_Qkey", adapt(JV_Qkey), DataTypes.StringType);
		registry.register("JV_AttachQuadMembers", JV_AttachQuadMembers,
							JarveyDataTypes.LongArray_Type.getSparkType());
		registry.register("JV_IsValidEnvelope", adapt(JV_IsValidEnvelope), DataTypes.BooleanType);
		registry.register("JV_IsValidWgs84Geometry", adapt(JV_IsValidWgs84Geometry), DataTypes.BooleanType);
		registry.register("JV_QKeyIntersects", JV_QKeyIntersects, DataTypes.BooleanType);
	}
	
	private static final GeometryFunction<Double> ST_Area = (geom) -> (geom != null) ? geom.getArea() : null;
	private static final UDF1<byte[],byte[]> ST_AsBinary = (wkb) -> wkb;
	private static final GeometryFunction<String> ST_AsText = (geom) -> (geom != null) ? GeoClientUtils.toWKT(geom) : null;
	private static final GeometryOperator ST_Boundary = geom -> (geom != null) ? geom.getBoundary() : null;
	private static final Arg1GeometryOperator<Double> ST_Buffer = (geom, dist) -> (geom != null) ? geom.buffer(dist) : null;
	
	private static final GeometryOperator ST_Centroid = (geom) -> (geom != null) ? geom.getCentroid() : null;
	private static final Arg1GeometryOperator<Integer> ST_ReducePrecision = (geom, factor) -> {
		if ( geom != null ) {
			GeometryPrecisionReducer reducer = GeoClientUtils.toGeometryPrecisionReducer(factor);
			return reducer.reduce(geom);
		}
		else {
			return null;
		}
	};
	private static final BinaryGeometryFunction<Boolean> ST_Contains
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.contains(geom2) : false;
	
	private static final GeometryOperator ST_ConvexHull = (geom) -> (geom != null) ? geom.convexHull() : null;
	private static final GeometryFunction<Integer> ST_CoordDim = (geom) -> (geom != null) ? geom.getDimension() : null;
	private static final BinaryGeometryFunction<Boolean> ST_Crosses
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.crosses(geom2) : false;
	private static final BinaryGeometryOperator ST_Difference 
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.difference(geom2) : null;

	private static final GeometryFunction<Integer> ST_Dimension = (geom) -> (geom != null) ? geom.getDimension() : null;
	private static final BinaryGeometryFunction<Boolean> ST_Disjoint
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.disjoint(geom2) : null;
	private static final BinaryGeometryFunction<Double> ST_Distance
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.distance(geom2) : null;
	
	private static final GeometryOperator ST_EndPoint = (geom) -> {
		if ( geom != null && Geometries.get(geom) == Geometries.LINESTRING ) {
			return ((LineString)geom).getEndPoint();
		}
		else {
			return null;
		}
	};
	
	private static final GeometryOperator ST_Envelope
					= (geom) -> (geom != null) ? GeometryUtils.toPolygon(geom.getEnvelopeInternal()) : null;
	private static final BinaryGeometryFunction<Boolean> ST_Equals
						= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.equals(geom2) : null;
	private static final Arg1BoxOperator<Double> ST_ExpandBox1 = (envl, dist) -> {
		if ( envl != null ) {
			envl.expandBy(dist);
			return envl;
		}
		else {
			return null;
		}
	};
	private static final Arg2BoxOperator<Double,Double> ST_ExpandBox2 = (envl, dx, dy) -> {
		if ( envl != null ) {
			envl.expandBy(dx, dy);
			return envl;
		}
		else {
			return null;
		}
	};
	private static final GeometryOperator ST_ExteriorRing = geom -> {
		if ( geom != null && Geometries.get(geom) == Geometries.POLYGON ) {
			return ((Polygon)geom).getExteriorRing();
		}
		else {
			return null;
		}
	};
	
	private static final GeometryFactory<String> ST_GeomFromText = wkt -> {
		if ( wkt == null ) {
			return null;
		}
		else {
			try {
				return GeoClientUtils.fromWKT((String)wkt);
			}
			catch ( ParseException e1 ) {
				throw new JarveyRuntimeException("fails to parse WKT: cause=" + e1);
			}
		}
	};
	
	private static final GeometryFactory<byte[]> ST_GeomFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		else {
			try {
				return GeoClientUtils.fromWKB(wkb);
			}
			catch ( ParseException e ) {
				throw new JarveyRuntimeException("fails to parse WKB: cause=" + e);
			}
		}
	};
	
	private static final Arg1GeometryOperator<Integer> ST_GeometryN
					= (geom, idx) -> (geom != null) ? geom.getGeometryN(idx-1) : null;
	private static final GeometryFunction<String> ST_GeometryType
						= geom -> (geom != null) ?  GeoClientUtils.getType(geom) : null;

	private static final Arg1GeometryOperator<Integer> ST_InteriorRingN
					= (geom, idx) -> (geom != null && geom instanceof Polygon)
										? ((Polygon)geom).getInteriorRingN(idx-1) : null;
	private static final BinaryGeometryOperator ST_Intersection
					= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.intersection(geom2) : null;
	private static final BinaryGeometryFunction<Boolean> ST_Intersects = (geom1, geom2) -> {
		return (geom1 != null && geom2 != null) ? geom1.intersects(geom2) : false;
	};
	private static final GeometryFunction<Boolean> ST_IsClosed = (geom) -> {
		if ( geom != null && Geometries.get(geom) == Geometries.LINESTRING ) {
			return ((LineString)geom).isClosed();
		}
		return false;
	};
	private static final GeometryFunction<Boolean> ST_IsRing = (geom) -> {
		if ( geom != null && Geometries.get(geom) == Geometries.LINESTRING ) {
			return ((LineString)geom).isRing();
		}
		return false;
	};

	private static final GeometryFunction<Boolean> ST_IsSimple = (geom) -> (geom != null) ? geom.isSimple() : false;
	private static final GeometryFunction<Boolean> ST_IsValid = (geom) -> (geom != null) ? geom.isValid() : false;
	private static final BoxFunction<Boolean> ST_IsValidEnvelope = (envl) -> {
		if ( envl != null ) {
			return (envl.getMaxY() <= 85 && envl.getMinY() >= -85
					&& envl.getMaxX() <= 180 && envl.getMinX() >= -180);
		}
		
		return false;
	};
	private static final GeometryFunction<Double> ST_Length = (geom) -> (geom != null) ? geom.getLength() : null;
	private static final GeometryFactory<String> ST_LineFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromText.create(wkt);
		return ( geom instanceof LineString ) ? geom : null;
	};
	private static final GeometryFactory<byte[]> ST_LineFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromWKB.create(wkb);
		return ( geom instanceof LineString ) ? geom : null;
	};
	private static final GeometryFactory<String> ST_MLineFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromText.create(wkt);
		return ( geom instanceof MultiLineString ) ? geom : null;
	};
	private static final GeometryFactory<byte[]> ST_MLineFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromWKB.create(wkb);
		return ( geom instanceof MultiLineString ) ? geom : null;
	};
	private static final GeometryFactory<String> ST_MPointFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromText.create(wkt);
		return ( geom instanceof MultiPoint ) ? geom : null;
	};
	private static final GeometryFactory<byte[]> ST_MPointFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromWKB.create(wkb);
		return ( geom instanceof MultiPoint ) ? geom : null;
	};
	private static final GeometryFactory<String> ST_MPolyFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromText.create(wkt);
		return ( geom instanceof MultiPolygon ) ? geom : null;
	};
	private static final GeometryFactory<byte[]> ST_MPolyFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromWKB.create(wkb);
		return ( geom instanceof MultiPolygon ) ? geom : null;
	};
	private static final GeometryFactory<String> ST_GeomCollFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromText.create(wkt);
		return ( geom instanceof GeometryCollection ) ? geom : null;
	};
	private static final GeometryFactory<byte[]> ST_GeomCollFromWKB = (wkb) -> {
		if ( wkb == null ) {
			return null;
		}
		Geometry geom = ST_GeomFromWKB.create(wkb);
		return ( geom instanceof GeometryCollection ) ? geom : null;
	};
	private static final BinaryGeometryOperator ST_MakeLine = (geom1, geom2) -> {
		Coordinate start, end;
		if ( geom1 == null || geom2 == null ) {
			return null;
		}
		
		if ( geom1 instanceof Point ) {
			start = ((Point)geom1).getCoordinate();
		}
		else if ( geom1 instanceof LineString ) {
			start = ((LineString)geom1).getStartPoint().getCoordinate();
		}
		else {
			throw new JarveyRuntimeException("ST_MakeLine takes the only Point/LineString, but=" + geom1);
		}
		
		if ( geom2 instanceof Point ) {
			end = ((Point)geom2).getCoordinate();
		}
		else if ( geom1 instanceof LineString ) {
			end = ((LineString)geom2).getStartPoint().getCoordinate();
		}
		else {
			throw new JarveyRuntimeException("ST_MakeLine takes the only Point/LineString, but=" + geom2);
		}

		return GeometryUtils.toLineString(start, end);
	};
	private static final UDF1<WrappedArray<byte[]>,byte[]> ST_MakeLineA = (wkbArray) -> {
		byte[][] wkbs = new byte[wkbArray.length()][];
		for ( int i =0; i < wkbs.length; ++i ) {
			wkbs[i] = wkbArray.apply(i);
		}
		GeometryArrayBean rows = new GeometryArrayBean(wkbs);
		Coordinate[] coords = FStream.of(rows.asGeometries())
									.cast(Point.class)
									.map(Point::getCoordinate)
									.toArray(Coordinate.class);
		LineString line = GeometryUtils.toLineString(coords);
		return GeometryBean.serialize(line);
	};
	
	private static final GeometryFunction<Integer> ST_NumGeometries = geom -> (geom != null) ? geom.getNumGeometries() : null;
	private static final GeometryFunction<Integer> ST_NumInteriorRings = geom -> {
		if ( geom != null && geom instanceof Polygon ) {
			return ((Polygon)geom).getNumInteriorRing();
		}
		else {
			return null;
		}
	};
	private static final GeometryFunction<Integer> ST_NumPoints = geom -> {
		if ( geom != null && geom instanceof LineString ) {
			return ((LineString)geom).getNumPoints();
		}
		else {
			return null;
		}
	};
	private static final BinaryGeometryFunction<Boolean> ST_OrderingEquals = (geom1, geom2) -> {
		if ( geom1 == null || geom2 == null ) {
			return false;
		}
		else {
			return geom1.equalsExact(geom2);
		}
	};
	private static final BinaryGeometryFunction<Boolean> ST_Overlaps = (geom1, geom2) -> {
		if ( geom1 == null || geom2 == null ) {
			return false;
		}
		else {
			return geom1.overlaps(geom2);
		}
	};
	
	private static final UDF2<Object,Object,byte[]> ST_Point = (xCol, yCol) -> {
		if ( xCol != null && yCol != null ) {
			double xpos = DataUtils.asDouble(xCol);
			double ypos = DataUtils.asDouble(yCol);
			
			Point pt = GeometryUtils.toPoint(xpos, ypos);
			return GeometryBean.serialize(pt);
		}
		else {
			return null;
		}
	};
	
	private static final GeometryFactory<String> ST_PointFromText = (wkt) -> {
		if ( wkt != null ) {
			Geometry geom = ST_GeomFromText.create(wkt);
			return ( geom instanceof Point ) ? geom : null;
		}
		else {
			return null;
		}
	};
	private static final GeometryFactory<byte[]> ST_PointFromWKB = (wkb) -> {
		if ( wkb != null ) {
			Geometry geom = ST_GeomFromWKB.create(wkb);
			return ( geom instanceof Point ) ? geom : null;
		}
		else {
			return null;
		}
	};
	
	private static final Arg1GeometryOperator<Integer> ST_PointN = (geom, index) -> {
		if ( geom != null && !geom.isEmpty() ) {
			Coordinate[] coords = geom.getCoordinates();
			if ( index >= 0 || index < coords.length ) {
				return GeometryUtils.toPoint(coords[index]);
			}
		}
		return null;
	};
	
	private static final GeometryFactory<String> ST_PolygonFromText = (wkt) -> {
		if ( wkt != null ) {
			Geometry geom = ST_GeomFromText.create(wkt);
			return ( geom instanceof Polygon ) ? geom : null;
		}
		else {
			return null;
		}
	};
	private static final GeometryFactory<byte[]> ST_PolygonFromWKB = (wkb) -> {
		if ( wkb != null ) {
			Geometry geom = ST_GeomFromWKB.create(wkb);
			return ( geom instanceof Polygon ) ? geom : null;
		}
		else {
			return null;
		}
	};
/*
	
	private static final UDF1<Object,byte[]> ST_PolygonFromText = (wkt) -> {
		if ( wkt == null ) {
			return null;
		}
		else if ( wkt instanceof String ) {
			Geometry geom = GeoClientUtils.fromWKT((String)wkt);
			if ( geom instanceof Polygon ) {
				return GeometryRow.serialize(geom);
			}
			
			return null;
		}
		else {
			s_logger.error("ST_PolygonFromText should take string type: " + wkt.getClass());
			throw new IllegalArgumentException();
		}
	};
*/

	private static final BinaryGeometryFunction<String> ST_RelatePattern = (geom1, geom2) -> {
		if ( geom1 != null && geom2 != null ) {
			return geom1.relate(geom2).toString();
		}
		else {
			return null;
		}
	};
	private static final Arg1BinaryGeometryFunction<String,Boolean> ST_Relate = (geom1, geom2, pat) -> {
		if ( geom1 != null && geom2 != null ) {
			return geom1.relate(geom2, pat);
		}
		else {
			return null;
		}
	};
	private static final GeometryOperator ST_StartPoint = (geom) -> {
		if ( geom != null && Geometries.get(geom) == Geometries.LINESTRING ) {
			return ((LineString)geom).getStartPoint();
		}
		else {
			return null;
		}
	};
	private static final BinaryGeometryOperator ST_SymDifference 
					= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.symDifference(geom2) : null;
	private static final BinaryGeometryFunction<Boolean> ST_Touches
					= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.touches(geom2) : false;
	private static final BinaryGeometryFunction<Boolean> ST_Within
					= (geom1, geom2) -> (geom1 != null && geom2 != null) ? geom1.within(geom2) : false;

/*
	private static final UDF2<byte[],byte[],byte[]> ST_Union = (wkb1, wkb2) -> {
		Geometry geom1 = GeometryRow.deserialize(wkb1);
		Geometry geom2 = GeometryRow.deserialize(wkb2);
		if ( geom1 == null ) {
			return null;
		}
		if ( geom2 == null ) {
			return null;
		}
		return GeometryRow.serialize(geom1.union(geom2));
	};
*/
	
	private static final GeometryFunction<Double> ST_X = geom -> {
		if ( geom == null || geom.isEmpty() ) {
			return null;
		}
		else if ( geom instanceof Point ) {
			return ((Point)geom).getX();
		}
		else {
			throw new JarveyRuntimeException("ST_X takes Point geometry but was " + GeoClientUtils.getType(geom));
		}
	};
	private static final BoxFunction<Double> ST_XMax = Envelope::getMaxX;
	private static final BoxFunction<Double> ST_XMin = Envelope::getMinX;
	private static final GeometryFunction<Double> ST_Y = geom -> {
		if ( geom == null || geom.isEmpty() ) {
			return null;
		}
		else if ( geom instanceof Point ) {
			return ((Point)geom).getY();
		}
		else {
			throw new JarveyRuntimeException("ST_Y takes Point geometry but was " + GeoClientUtils.getType(geom));
		}
	};
	private static final BoxFunction<Double> ST_YMax = Envelope::getMaxY;
	private static final BoxFunction<Double> ST_YMin = Envelope::getMinY;
	
	private static final GeometryFunction<Double[]> Box2D = (geom) -> {
		Envelope envl = (geom != null) ? geom.getEnvelopeInternal() : null;
		return SERDE_ENVELOPE.serialize(envl);
	};
	
	
/*	
	private static final UDF2<Envelope,Envelope,Boolean> ST_EnvelopeIntersects = (envl1, envl2) -> {
		if ( envl1 != null && envl2 != null ) {
			return envl1.intersects(envl2);
		}
		else {
			return false;
		}
	};
*/
	private static ThreadLocal<Map<Tuple<Integer,Integer>, CoordinateTransform>> TRANSFORM_CACHE
																					= new ThreadLocal<>();
	private static final UDF3<byte[],Integer,Integer,byte[]> ST_Transform = (wkb, fromSrid, toSrid) -> {
		Geometry geom = GeometryBean.deserialize(wkb);
		if ( geom == null ) {
			return null;
		}
		else {
			Map<Tuple<Integer,Integer>, CoordinateTransform> cache = TRANSFORM_CACHE.get();
			if ( cache == null ) {
				TRANSFORM_CACHE.set(cache = new HashMap<>());
			}
			CoordinateTransform trans = cache.get(Tuple.of(fromSrid, toSrid));
			if ( trans == null ) {
				String srid1Str = String.format("EPSG:%d", fromSrid);
				String srid2Str = String.format("EPSG:%d", toSrid);
				trans = CoordinateTransform.get(srid1Str, srid2Str);
				cache.put(Tuple.of(fromSrid, toSrid), trans);
			}
			
			Geometry transformed = trans.transform(geom);
			return GeometryBean.serialize(transformed);
		}
	};
	
	private static final Arg2BoxOperator<Integer, Integer> ST_TransformBox = (envl, fromSrid, toSrid) -> {
		if ( envl == null ) {
			return null;
		}
		
		Map<Tuple<Integer,Integer>, CoordinateTransform> cache = TRANSFORM_CACHE.get();
		if ( cache == null ) {
			TRANSFORM_CACHE.set(cache = new HashMap<>());
		}
		CoordinateTransform trans = cache.get(Tuple.of(fromSrid, toSrid));
		if ( trans == null ) {
			String srid1Str = String.format("EPSG:%d", fromSrid);
			String srid2Str = String.format("EPSG:%d", toSrid);
			trans = CoordinateTransform.get(srid1Str, srid2Str);
			cache.put(Tuple.of(fromSrid, toSrid), trans);
		}
		
		return trans.transform(envl);
	};
	
/*
//	private static final UDF1<String,Geometry> ST_ParseDateTime = (str, pattern) -> {
//		if ( str == null ) {
//			return null;
//		}
//		else if ( str instanceof String ) {
//			LocalDateTime ldt = DateTimeFunctions.DateTimeParse(dtStr, pattern);
//		}
//		else {
//			s_logger.error("ST_GeomFromText should take string type: " + wktStr.getClass());
//			throw new IllegalArgumentException();
//		}
//	};
	
	private static final UDF1<String,Geometry> ST_GeomFromGeoJSON = (json) -> {
		try {
			if ( json == null ) {
				return null;
			}
			return GeoJsonReader.read(json);
		}
		catch ( Exception e ) {
			s_logger.error(String.format("fails to parse GeoJSON: '%s'", json), e);
			return null;
		}
	};
	
	private static final UDF1<Geometry,String> ST_AsGeoJSON = (geom) -> {
		if ( geom == null ) {
			return null;
		}

		return new GeometryJSON().toString(geom);
	};
*/
	
	private static final Long[] OUTLIER_MEMBERS = new Long[]{MapTile.OUTLIER_QID};
	private static final UDF2<WrappedArray<Double>,WrappedArray<Long>,Long[]>
	JV_AttachQuadMembers = (coords, candidates) -> {
		Envelope envl = SERDE_ENVELOPE.deserialize(coords);
		if ( envl != null ) {
			long[] qids = JarveyArrayType.unwrapLongArray(candidates);
			Coordinate refPt = envl.centre();
			Long[] members = FStream.of(qids)
									.filter(q -> q != MapTile.OUTLIER_QID)
									.map(MapTile::fromQuadId)
									.filter(tile -> tile.intersects(envl))
									.map(tile -> tile.contains(refPt)
													? Tuple.of(tile.getQuadId(), 1)
													: Tuple.of(tile.getQuadId(), 0))
									.sort((t1, t2) -> Integer.compare(t2._2, t1._2))
									.map(t -> t._1)
									.toArray(Long.class);
			return ( members.length > 0 ) ? members : OUTLIER_MEMBERS;
		}
		else {
			return OUTLIER_MEMBERS;
		}
	};

	private static final GeometryFunction<Boolean> JV_IsValidWgs84Geometry = (geom) -> {
		return !FStream.of(geom.getCoordinates())
						.exists(coord -> {
							return coord.x < -180 || coord.x > 180
								|| coord.y < -85 || coord.y > 85;
						});
	};
	private static final BoxFunction<Boolean> JV_IsValidEnvelope = (envl) -> {
		if ( envl != null ) {
			return envl.getMinY() >= -85 && envl.getMaxY() <= 85
				&& envl.getMinX() >= -180 && envl.getMaxX() <= 180;
		}
		else {
			return false;
		}
	};
	
	private static final BoxFunction<String> JV_Qkey = (envl) -> {
		if ( envl != null ) {
			return MapTile.getSmallestContainingTile(envl, 31).getQuadKey();
		}
		else {
			return MapTile.OUTLIER_QKEY;
		}
	};
	
	private static final UDF2<String,String,Boolean> JV_QKeyIntersects = (qkey1, qkey2) -> {
		if ( qkey1.equals(qkey2) ) {
			return true;
		}
		else {
			int prefixLen = StringUtils.getCommonPrefix(qkey1, qkey2).length();
			return prefixLen == qkey1.length() || prefixLen == qkey2.length();
		}
	};
	
	private static final UDF2<WrappedArray<Double>,WrappedArray<Double>,Boolean>
	ST_BoxIntersectsBox = (coords1, coords2) -> {
		Envelope envl1 = SERDE_ENVELOPE.deserialize(coords1); 
		Envelope envl2 = SERDE_ENVELOPE.deserialize(coords2); 
		if ( envl1 != null && envl2 != null ) {
			return envl1.intersects(envl2);
		}
		else {
			return false;
		}
	};
	
	private static final UDF2<byte[],WrappedArray<Double>,Boolean> ST_GeomIntersectsBox = (wkb, coords) -> {
		Geometry geom = GeoClientUtils.fromWKB(wkb);
		Envelope range = SERDE_ENVELOPE.deserialize(coords); 
		if ( geom != null && range != null ) {
			if ( geom instanceof Point ) {
				return range.contains(geom.getCoordinate());
			}
			else {
				Envelope envl = geom.getEnvelopeInternal();
				if ( envl.intersects(range) ) {
					return geom.intersects(GeometryUtils.toPolygon(range));
				}
				else {
					return false;
				}
			}
		}
		else {
			return false;
		}
	};
	
	private static final Arg1GeometryFunction<byte[],Boolean> ST_IntersectsGeom = (geom, wkb) -> {
		if ( geom != null ) {
			try {
				Geometry arg = GeoClientUtils.fromWKB(wkb);
				return geom.intersects(arg);
			}
			catch ( ParseException e ) {
				throw new JarveyRuntimeException("fails to parse WKB: cause=" + e);
			}
		}
		else {
			return false;
		}
	};
	
	private static final Arg1GeometryFunction<byte[],Boolean> TEST_Intersects = (geom, wkb) -> {
		if ( geom == null ) {
			return false;
		}
		
		try {
			Geometry arg = GeoClientUtils.fromWKB(wkb);
			if ( !geom.intersects(arg) ) {
				return false;
			}

			GeometryPrecisionReducer reducer = GeoClientUtils.toGeometryPrecisionReducer(2);
			geom = reducer.reduce(geom);
			geom = geom.intersection(arg);
			double area = geom.getArea();
			return area > 5;
		}
		catch ( ParseException e ) {
			throw new JarveyRuntimeException("fails to parse WKB: cause=" + e);
		}
	};
}
