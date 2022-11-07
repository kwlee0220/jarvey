package jarvey.appls;

import static jarvey.optor.geom.join.SpatialJoinOptions.ARC_CLIP;
import static jarvey.optor.geom.join.SpatialJoinOptions.SEMI_JOIN;
import static org.apache.spark.sql.functions.col;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTenMinutePolicyDetails {
	private static final String ELDERLY_CARE = "POI/노인복지시설";
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String POP_DENSITY = "주민/인구밀도_2000";
	private static final String HDONG = "구역/행정동코드";
	private static final String TEMP_RESULT = "tmp/10min/temp";
	private static final String RESULT = "tmp/10min/elderly_care_candidates";
	
	private static final String ELDERLY_CARES = "tmp/10min/elderly_cares";
	private static final String ELDERLY_CARE_BUFFER = "tmp/10min/elderly_care_buffers";
	private static final String NO_ELDERLY_CADASTRALS = "tmp/10min/no_elderly_cadastrals";
	private static final String HIGH_DENSITY_POP_CENTERS = "tmp/10min/high_density_pop_centers";
	private static final String HIGH_DENSITY_POP = "tmp/10min/high_density_pop";
	private static final String HIGH_DENSITY_HDONG = "tmp/10min/high_density_hdong";
	
	public static final void main(String... args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[7]")
											.getOrCreate();
		
		StopWatch watch = StopWatch.start();
		
		analysis01(jarvey);
		analysis02(jarvey);
		analysis03(jarvey);
		analysis05(jarvey);
		analysis04(jarvey);
		analysis06(jarvey);
		analysis07(jarvey);
		
		analysis08(jarvey);
		jarvey.deleteSpatialDataFrame(TEMP_RESULT);
		jarvey.deleteSpatialDataFrame(ELDERLY_CARES);
		jarvey.deleteSpatialDataFrame(ELDERLY_CARE_BUFFER);
		jarvey.deleteSpatialDataFrame(NO_ELDERLY_CADASTRALS);
		jarvey.deleteSpatialDataFrame(HIGH_DENSITY_POP);
		jarvey.deleteSpatialDataFrame(HIGH_DENSITY_POP_CENTERS);
		jarvey.deleteSpatialDataFrame(HIGH_DENSITY_HDONG);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SpatialDataFrame reloaded = jarvey.read().dataset(RESULT);
		reloaded.drop("the_geom").show(5);
		
		System.out.printf("완료: '경로당필요지역' 추출, elapsed=%s%n",
							watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis01(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();
		
		// 노인복지시설에서 경로당 위치를 추출함
		SpatialDataFrame elderly = jarvey.read().dataset(ELDERLY_CARE)
										.filter(col("induty_nm").equalTo("경로당"));
		elderly.writeSpatial().force(true).dataset(ELDERLY_CARES);
		watch.stop();
		
		long count = jarvey.read().dataset(ELDERLY_CARES).count();
		System.out.printf("완료: '영역분석', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis02(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();
		
		// 노인복지시설에서 경로당 위치를 추출함
		SpatialDataFrame elderly = jarvey.read().dataset(ELDERLY_CARES)
												.select("the_geom")
												.buffer(400);
		elderly.writeSpatial().force(true).dataset(ELDERLY_CARE_BUFFER);
		watch.stop();
		
		long count = jarvey.read().dataset(ELDERLY_CARE_BUFFER).count();
		System.out.printf("완료: '버퍼분석', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis03(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();

		// 지적 중에서 주변 400미터내에 경로당이 없는 지적만 추출
		SpatialDataFrame elderly = jarvey.read().dataset(ELDERLY_CARE_BUFFER);
		SpatialDataFrame noElderly = jarvey.read().dataset(CADASTRAL)
											.spatialBroadcastJoin(elderly, SEMI_JOIN().negated());
		noElderly.writeSpatial().force(true).dataset(NO_ELDERLY_CADASTRALS);
		watch.stop();
		
		long count = jarvey.read().dataset(NO_ELDERLY_CADASTRALS).count();
		System.out.printf("완료: '교차반전', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis04(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();
		
		// 인구수가 10000이상인 '인구밀도_2017' 지역의 중심점 추출함.
		SpatialDataFrame hdCenters = jarvey.read().dataset(HIGH_DENSITY_POP)
												.select("the_geom")
												.centroid();
		hdCenters.writeSpatial().force(true).dataset(HIGH_DENSITY_POP_CENTERS);
		watch.stop();
		
		long count = jarvey.read().dataset(HIGH_DENSITY_POP_CENTERS).count();
		System.out.printf("완료: '중심점 추출', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis05(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();
		
		// 인구수가 10000이상인 '인구밀도_2017' 지역 추출함.
		SpatialDataFrame highDensity = jarvey.read().dataset(POP_DENSITY)
											.filter("value >= 10000");
		highDensity.writeSpatial().force(true).dataset(HIGH_DENSITY_POP);
		watch.stop();
		
		long count = jarvey.read().dataset(HIGH_DENSITY_POP).count();
		System.out.printf("완료: '영역분석', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis06(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();

		// 인구수가 10000이상인 인구밀도 중심점을 포함한 행정동 위치 추출
		SpatialDataFrame hdCenters = jarvey.read().dataset(HIGH_DENSITY_POP_CENTERS);
		SpatialDataFrame highDensityHDong = jarvey.read().dataset(HDONG)
													.select("the_geom")
													.spatialBroadcastJoin(hdCenters, SEMI_JOIN());
		highDensityHDong.writeSpatial().force(true).dataset(HIGH_DENSITY_HDONG);
		watch.stop();
		
		long count = jarvey.read().dataset(HIGH_DENSITY_HDONG).count();
		System.out.printf("완료: '교차분석', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis07(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();

		// 결과 지적들 중에서 인구수 10000이상인 행정동 영역과 겹치는 부분 추출
		SpatialDataFrame noElderlyCadastral = jarvey.read().dataset(NO_ELDERLY_CADASTRALS);
		SpatialDataFrame hdHDong = jarvey.read().dataset(HIGH_DENSITY_HDONG);
		SpatialDataFrame result = noElderlyCadastral
										.spatialBroadcastJoin(hdHDong, ARC_CLIP());
		result.writeSpatial().force(true).dataset(TEMP_RESULT);
		watch.stop();
		
		long count = jarvey.read().dataset(TEMP_RESULT).count();
		System.out.printf("완료: '클립분석', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
	
	private static final void analysis08(JarveySession jarvey) {
		StopWatch watch = StopWatch.start();

		jarvey.read().dataset(TEMP_RESULT)
				.coalesce(3)
				.writeSpatial().force(true).dataset(RESULT);
		watch.stop();
		
		long count = jarvey.read().dataset(RESULT).count();
		System.out.printf("완료: '결과정리', count=%d, elapsed=%s%n",
							count, watch.stopAndGetElpasedTimeString());
	}
}
