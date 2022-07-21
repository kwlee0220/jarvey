package jarvey.test;

import static jarvey.jarvey_functions.tp_duration;
import static jarvey.jarvey_functions.tp_npoints;
import static jarvey.jarvey_functions.tp_path;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.round;

import jarvey.JarveySession;
import jarvey.SpatialDataset;

public class TestAnalysis {
	public static final void main(String[] args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		String dsId = "나비콜/일별로그";
		SpatialDataset sds = jarvey.read().dataset(dsId);

		sds = sds.withRegularColumn("npoints", tp_npoints(col("location")))
				.withRegularColumn("duration", tp_duration(col("location")).divide(1000))
				.withRegularColumn("path_length", round(tp_path("location"),0))
				.drop("location");
		System.out.println(sds.count());
		
		sds.select("carno", "driver_id", "day", "npoints").orderBy(desc("npoints")).limit(1).show();
		sds.select("carno", "driver_id", "day", "duration").orderBy(desc("duration")).limit(1).show();
		sds.select("carno", "driver_id", "day", "path_length").orderBy(desc("path_length")).limit(1).show();
		
		jarvey.spark().stop();
	}
}
