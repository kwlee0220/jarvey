package jarvey.sample.dtg;

import static org.apache.spark.sql.functions.count;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;

import utils.StopWatch;
import utils.UnitUtils;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class FindIntervalAndSplitOnIt {
	private static final String INPUT = "tmp/dtg_raw";
	private static final String OUTPUT = "tmp/xxx";
	
	private static final long SEGMENT_MILLIS = UnitUtils.parseDuration("10m");
	private static final int GROUP_PARTITION_COUNT = 500;
	private static final String TEMPORAL_POINT_COLUMN = "tpoint";
	private static final long TRAJ_SPLIT_INTERVAL = UnitUtils.parseDuration("1m");
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("build_temporal_points_from_dtg")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		
		Dataset<Row> base = jarvey.read().dataset(INPUT)
								.toDataFrame()
								.groupBy("trans_reg_num", "driver_code")
									.agg(count("*").as("count"))
								.cache();

		System.out.println("count=" + base.count());
		System.out.println("car count=" + base.select("trans_reg_num").distinct().count());
		System.out.println("driver count=" + base.select("driver_code").distinct().count());
		
		Dataset<Row> rows = base.orderBy(base.col("count"));
		rows.printSchema();
		SpatialDataFrame sdf = jarvey.toSpatial(rows);
		
		sdf.writeSpatial().force(true).dataset("tmp/xxx");
		
		System.in.read();
		jarvey.spark().stop();
	}
}
