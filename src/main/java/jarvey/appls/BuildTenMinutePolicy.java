package jarvey.appls;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SaveMode;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTenMinutePolicy {
	private static final String ELDERLY_CARE = "poi_elderly_care_facilities";
	private static final String CADASTRAL = "district_cadastral";
	private static final String POP_DENSITY = "population_density_2000";
	private static final String HDONG = "district_adminstral";
	
	private static final String ELDERLY_CARE_BUFFER = "tmp_10min_eldely_care_facilites_bufferred";
	private static final String HIGH_DENSITY_CENTER = "tmp_10min_high_density_center";
	private static final String HIGH_DENSITY_HDONG = "tmp_10min_high_density_hdong";
	private static final String RESULT = "tmp/10min/elderly_care_candidates";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[10]")
											.getOrCreate();
		
		StopWatch watch = StopWatch.start();
//		
		// '노인복지시설_경로당_추출_버퍼' 추출
		StopWatch watch2 = StopWatch.start();
		bufferElderlyCareFacilities(jarvey);
		System.out.println("완료: '노인복지시설_경로당_추출_버퍼' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// '인구밀도_2017_중심점추출_10000이상' 추출
		watch2 = StopWatch.start();
		findHighPopulationDensity(jarvey);
		System.out.println("완료: '인구밀도_2017_중심점추출_10000이상' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// 인구밀도_10000이상_행정동추출
		watch2 = StopWatch.start();
		findHighPopulationHDong(jarvey);
		System.out.println("완료: '인구밀도_10000이상_행정동' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());

//		watch2 = StopWatch.start();
//		DataSet ds = marmot.getDataSet(CADASTRAL);
//		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
//
//		Plan plan = Plan.builder("경로당필요지역추출")
//						.load(CADASTRAL, LoadOptions.FIXED_MAPPERS)
////						.load(CADASTRAL)
//						.spatialSemiJoin(gcInfo.name(), ELDERLY_CARE_BUFFER,	// (3) 교차반전
//										SpatialJoinOptions.NEGATED)
//						.arcClip(gcInfo.name(), HIGH_DENSITY_HDONG)			// (7) 클립분석
//						.store(RESULT, FORCE(gcInfo))
//						.build();
//		marmot.execute(plan);
//		watch.stop();
//		System.out.println("완료: '경로당필요지역' 추출, elapsed="
//							+ watch2.stopAndGetElpasedTimeString());
//		
//		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
//		output = marmot.getDataSet(RESULT);
//		SampleUtils.printPrefix(output, 5);
//
//		System.out.printf("count=%d, total elapsed time=%s%n",
//							output.getRecordCount(), watch.getElapsedMillisString());
	}
	
	private static void bufferElderlyCareFacilities(JarveySession jarvey) {
		SpatialDataset sds = jarvey.read().dataset(ELDERLY_CARE)
									.filter(col("induty_nm").equalTo("경로당"))
									.buffer(400);
		sds.writeSpatial().mode(SaveMode.Overwrite).dataset(ELDERLY_CARE_BUFFER);
	}
	
	private static void findHighPopulationDensity(JarveySession jarvey) {
		SpatialDataset sds = jarvey.read().dataset(POP_DENSITY)
									.filter(col("value").geq(10000))
									.centroid();
		sds.writeSpatial().mode(SaveMode.Overwrite).dataset(HIGH_DENSITY_CENTER);
	}
	
	private static void findHighPopulationHDong(JarveySession jarvey) {
		SpatialDataset highDensity = jarvey.read().dataset(HIGH_DENSITY_CENTER);
		SpatialDataset sds = jarvey.read().dataset(HDONG)
									.spatialSemiJoin(highDensity);	// (6) 교차분석
		sds.writeSpatial().mode(SaveMode.Overwrite).dataset(HIGH_DENSITY_HDONG);
	}
}
