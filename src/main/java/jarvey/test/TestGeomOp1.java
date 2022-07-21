package jarvey.test;

import static org.apache.spark.sql.functions.col;

import jarvey.JarveySession;
import jarvey.SpatialDataset;

public class TestGeomOp1 {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		SpatialDataset df = jarvey.read().dataset("구역/연속지적도");
		
//		df = spatial(df).buffer(5);
//		df = spatial(df).centroid();
//		df = spatial(df).geometry_type("type").select(col("type"));
//		df = spatial(df).length("length").select(col("length"));
//		df = spatial(df).area("area").select(col("area"));
//		df = spatial(df).to_wkb("wkb").select(length(col("wkb")));
//		df = spatial(df).to_wkt("wkt").select(col("wkt"));
//		df = spatial(df).is_valid("valid").select(col("valid"));
//		df = spatial(df).is_ring("is_ring").select(col("is_ring"));
//		df = spatial(df).centroid("overlap")
//						.default_column("overlap")
//							.buffer(100)
//							.intersection_with(col("the_geom"))
//							.geometry_type("type")
//							.select(col("type"));
//		df = spatial(df).buffer(1, "buffer").intersects_with(col("buffer"), "flag").select(col("flag"));
//		df = spatial(df).num_geometries("count").select(col("count"));
//		df = spatial(df).geometry_at(1).geometry_type("type").select(col("type"));
//		df = spatial(df).centroid("center").contains(col("center"), "res").select(col("res"));
//		df = df.select(callUDF("ST_ConvexHull", col("the_geom")).as("the_geom"));
//		df = df.select(callUDF("ST_Envelope", col("the_geom")).as("the_geom"));
//		df = spatial(df).centroid().to_x("x").to_y("y").select(col("x"), col("y"));
//		df = spatial(df).convexhull().geometry_type("type").select(col("type"));
//		df = spatial(df).coord_dim("dim").select(col("dim"));
//		df = spatial(df).srid("srid").select(col("srid"));
		df = df.centroid().transform(4326).to_x("x").srid("srid").select(col("x"), col("srid"));
		
		df.printSchema();
		df.show(5);
		
		jarvey.spark().stop();
	}
}
