package jarvey.test;

import org.apache.hadoop.fs.Path;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

import utils.stream.FStream;

public class TestGeomOp1 {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey")
											.master("local[5]")
											.getOrCreate();
		
		SpatialDataFrame sdf = jarvey.read().dataset("구역/연속지적도");
		
//		sdf = sdf.buffer(5, GeomOpOptions.DEFAULT).limit(5);
		sdf = sdf.centroid().limit(5);
//		sdf = sdf.box2d("box").select("box").limit(5);
//		df = spatial(df).geometry_type("type").select(col("type"));
//		df = spatial(df).length("length").select(col("length"));
//		sdf = sdf.area("area").drop("the_geom").limit(5);
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
//		df = spatial(df).convexhull().geometry_type("type").select(col("type"));
//		df = spatial(df).coord_dim("dim").select(col("dim"));
//		df = spatial(df).srid("srid").select(col("srid"));
//		sdf = sdf.centroid()
//					.transformCrs(4326, GeomOpOptions.DEFAULT)
//					.to_xy("x", "y")
//					.srid("srid")
//					.select(col("x"), col("srid"));
		
		sdf.printSchema();
		FStream.from(sdf.collectAsRecordList()).take(5).forEach(System.out::println);
		
		jarvey.spark().stop();
	}
}
