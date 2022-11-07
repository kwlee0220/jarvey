package jarvey.sample.dtg;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.type.temporal.build.BuildTemporalPoint;

import utils.StopWatch;
import utils.UnitUtils;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class BuildTemporalPointFromDtg {
//	private static final String DTG_2018 = "교통/DTG/2018";
//	private static final String DTG_2018 = "tmp/dtg_2018";
	private static final String DTG_2018 = "교통/DTG/test";
//	private static final String DTG_2018 = "교통/DTG/test_tiny";
	private static final String QIDS = "구역/연속지적도_2019";
	private static final String TEMP_BASE = "tmp/dtg_base";
	private static final String TEMP_RAW = "tmp/dtg_raw";
	private static final String TEMP_IDX = "tmp/idx";
	private static final String OUTPUT = "tmp/dtg_tpoints";

	private static final long PERIOD_MILLIS = UnitUtils.parseDuration("30m");
//	private static final int PART_COUNT = 1000;
	private static final int PART_COUNT = 3;
	
	public static final void main(String... args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("build_temporal_points_from_dtg")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		
		String[] keyColNames = new String[]{"trans_reg_num", "driver_code"};
		String[] xytColNames = new String[]{"x", "y", "ts"};
		BuildTemporalPoint app = new BuildTemporalPoint(jarvey, keyColNames, xytColNames, OUTPUT);
		app.setPeriod(PERIOD_MILLIS);
		app.setMergingPartitionCount(PART_COUNT);
		
		SpatialDataFrame sdf = jarvey.read().dataset(DTG_2018);
		Dataset<Row> input = sdf.to_xy("x", "y").toDataFrame();
		app.run(input);
		
		watch.stop();
		
		long count = jarvey.read().dataset(OUTPUT).count();
		System.out.printf("count=%d, elapsed=%s%n", count, watch.getElapsedMillisString());
		
		System.in.read();
		
		jarvey.spark().stop();
	}
}
