package jarvey;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.type.EnvelopeValue;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;
import jarvey.type.JarveySchema;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class jarvey_functions {
	public static final Column the_geom() {
		return col("the_geom");
	}

	public static final SpatialDataset spatial(JarveySession jarvey, Dataset<Row> df, GeometryColumnInfo gcInfo) {
		JarveySchema jschema = FStream.of(df.schema().fields())
										.foldLeft(JarveySchema.builder(), (builder, field) -> {
											if ( field.name().equalsIgnoreCase(gcInfo.name()) ) {
												GeometryType geomType = GeometryType.of(gcInfo.srid());
												return builder.addJarveyColumn(field.name(), geomType);
											}
											else {
												return builder.addRegularColumn(field.name(), field.dataType());
											}
										})
										.build();
		return new SpatialDataset(jarvey, df, jschema);
	}
	
	public static final Column ST_Area(Column geomCol) {
		return callUDF("ST_Area", geomCol);
	}
	public static final Column ST_Length(Column geomCol) {
		return callUDF("ST_Length", geomCol);
	}
	public static final Column ST_CoordDim(Column geomCol) {
		return callUDF("ST_CoordDim", geomCol);
	}
	public static final Column ST_AsBinary(Column geomCol) {
		return callUDF("ST_AsBinary", geomCol);
	}
	public static final Column ST_AsText(Column geomCol) {
		return callUDF("ST_AsText", geomCol);
	}
	public static final Column ST_X(Column geomCol) {
		return callUDF("ST_X", geomCol);
	}
	public static final Column ST_Y(Column geomCol) {
		return callUDF("ST_Y", geomCol);
	}
	public static final Column ST_GeometryType(Column geomCol) {
		return callUDF("ST_GeometryType", geomCol);
	}
	public static final Column ST_NumGeometries(Column geomCol) {
		return callUDF("ST_NumGeometries", geomCol);
	}
	public static final Column ST_Relate(Column leftGeomCol, Column rightGeomCol, Column patCol) {
		return callUDF("ST_Relate", leftGeomCol, rightGeomCol, patCol);
	}
	public static final Column ST_Relate(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_RelatePattern", leftGeomCol, rightGeomCol);
	}
	
	public static final Column ST_IsClosed(Column geomCol) {
		return callUDF("ST_IsClosed", geomCol);
	}
	public static final Column ST_IsRing(Column geomCol) {
		return callUDF("ST_IsRing", geomCol);
	}
	public static final Column ST_IsSimple(Column geomCol) {
		return callUDF("ST_IsSimple", geomCol);
	}
	public static final Column ST_IsValid(Column geomCol) {
		return callUDF("ST_IsValid", geomCol);
	}
	public static final Column ST_IsValidEnvelope(Column boxCol) {
		return callUDF("JV_IsValidEnvelope", boxCol);
	}

	public static final Column ST_Point(Column xCol, Column yCol) {
		return callUDF("ST_Point", xCol, yCol);
	}
	public static final Column ST_Buffer(Column geomCol, double radius) {
		return callUDF("ST_Buffer", geomCol, lit(radius));
	}
	public static final Column ST_Centroid(Column geomCol) {
		return callUDF("ST_Centroid", geomCol);
	}
	public static final Column ST_StartPoint(Column geomCol) {
		return callUDF("ST_StartPoint", geomCol);
	}
	public static final Column ST_EndPoint(Column geomCol) {
		return callUDF("ST_EndPoint", geomCol);
	}
	public static final Column ST_Transform(Column geomCol, int fromSrid, int toSrid) {
		if ( fromSrid != toSrid ) {
			return callUDF("ST_Transform", geomCol, lit(fromSrid), lit(toSrid));
		}
		else {
			return geomCol;
		}
	}
	public static final Column ST_GeometryN(Column geomCol, Column indexCol) {
		return callUDF("ST_GeometryN", geomCol, indexCol);
	}
	public static final Column ST_GeometryN(Column geomCol, int index) {
		return ST_GeometryN(geomCol, lit(index));
	}
	public static final Column ST_ConvexHull(Column geomCol) {
		return callUDF("ST_ConvexHull", geomCol);
	}

	public static final Column ST_GeomFromText(Column wkt) {
		return callUDF("ST_GeomFromText", wkt);
	}
	public static final Column ST_GeomFromWKB(Column wkb) {
		return wkb;
	}

	
	public static final Column ST_Contains(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Contains", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Contains(Column leftGeomCol, Geometry rightGeom) {
		return ST_Contains(leftGeomCol, lit(GeometryValue.serialize(rightGeom)));
	}
	public static final Column ST_Intersects(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Intersects", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Intersects(Column leftGeomCol, Geometry rightGeom) {
		return ST_Intersects(leftGeomCol, lit(GeometryValue.serialize(rightGeom)));
	}
	public static final Column ST_Crosses(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Crosses", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Equals(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Equals", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Equals(Column leftGeomCol, Geometry rightGeom) {
		return callUDF("ST_Equals", leftGeomCol, lit(GeometryValue.serialize(rightGeom)));
	}
	public static final Column ST_Intersection(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Intersection", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Intersection(Column leftGeomCol, Geometry rightGeom) {
		return ST_Intersection(leftGeomCol, lit(GeometryValue.serialize(rightGeom)));
	}
	public static final Column ST_Difference(Column leftGeomCol, Column rightGeomCol) {
		return callUDF("ST_Difference", leftGeomCol, rightGeomCol);
	}
	public static final Column ST_Difference(Column leftGeomCol, Geometry rightGeom) {
		return ST_Difference(leftGeomCol, lit(GeometryValue.serialize(rightGeom)));
	}

	
	public static final Column ST_Box2d(Column geomCol) {
		return callUDF("Box2D", geomCol);
	}
	public static final Column ST_BoxIntersects(Column leftBoxCol, Column rightBoxCol) {
		return callUDF("ST_BoxIntersects", leftBoxCol, rightBoxCol);
	}
	public static final Column ST_BoxIntersects(Column leftBoxCol, Envelope envl) {
		Double[] coords = (envl != null) ? EnvelopeValue.toCoordinates(envl) : null;
		return callUDF("ST_BoxIntersects", leftBoxCol, lit(coords));
	}
	public static final Column ST_TransformBox(Column boxCol, Column fromSrid, Column toSrid) {
		return callUDF("ST_TransformBox", boxCol, fromSrid, toSrid);
	}
	public static final Column ST_ExpandBox(Column boxCol, Column distCol) {
		return callUDF("ST_ExpandBox1", boxCol, distCol);
	}
	public static final Column ST_ExpandBox(Column boxCol, Column dxCol, Column dyCol) {
		return callUDF("ST_ExpandBox2", boxCol, dxCol, dyCol);
	}
	
	
	public static final Column JV_AttachQuadMembers(Column envl4326Col, Long[] candidates) {
		return callUDF("JV_AttachQuadMembers", envl4326Col, lit(candidates));
	}
	
	public static final Column tp_path(Column col) {
		return callUDF("TP_Path", col);
	}
	public static final Column tp_path(String col) {
		return tp_path(col(col));
	}
	
	public static final Column tp_duration(Column col) {
		return callUDF("TP_Duration", col);
	}
	
	public static final Column tp_npoints(Column col) {
		return callUDF("TP_NumPoints", col);
	}
}
