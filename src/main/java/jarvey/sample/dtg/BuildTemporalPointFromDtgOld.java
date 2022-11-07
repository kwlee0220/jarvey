package jarvey.sample.dtg;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

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
public class BuildTemporalPointFromDtgOld {
	private static final String DTG_2018 = "교통/DTG/2018";
//	private static final String DTG_2018 = "교통/DTG/test";
	
	private static final String DATASET_TRAJECTORIES = "tmp/dtg_trajectories";
	private static final String TEMPORAL_POINT_COLUMN = "tpoint";
	private static final int GROUP_PARTITION_COUNT = 500;
	private static final long TRAJ_SPLIT_INTERVAL = UnitUtils.parseDuration("1m");
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("build_temporal_points_from_dtg")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		
		Dataset<Row> rows = jarvey.read().dataset(DTG_2018)
//									.filter(JV_IsValidWgs84Geometry(col("the_geom")))
									.toDataFrame()
//									.limit(1000000)
									.filter(col("the_geom").isNotNull())
									.selectExpr("trans_reg_num", "driver_code",
												"ST_X(the_geom) as x", "ST_Y(the_geom) as y",
												"unix_timestamp(ts) as ts")
									.repartition(GROUP_PARTITION_COUNT, col("trans_reg_num"), col("driver_code"))
									.groupBy("trans_reg_num", "driver_code")
										.agg(callUDF("TP_BuildTemporalPoint", col("x"), col("y"),
														col("ts")).as(TEMPORAL_POINT_COLUMN));
		rows = rows.withColumn(TEMPORAL_POINT_COLUMN,
								callUDF("TP_SplitByInterval", col(TEMPORAL_POINT_COLUMN), lit(TRAJ_SPLIT_INTERVAL)));
		rows = rows.withColumn(TEMPORAL_POINT_COLUMN, explode(rows.col(TEMPORAL_POINT_COLUMN)));
		
		SpatialDataFrame sdf = jarvey.toSpatial(rows);
		sdf.printSchema();
		
		sdf.writeSpatial().force(true).dataset(DATASET_TRAJECTORIES);
		watch.stop();
		
		sdf = jarvey.read().dataset(DATASET_TRAJECTORIES);
		long count = sdf.count();
		long ngroups = sdf.groupBy("trans_reg_num", "driver_code").count().count();
		
		System.out.printf("count=(%d, %d), elapsed=%s%n", ngroups, count, watch.getElapsedMillisString());
		
		jarvey.spark().stop();
	}
}
