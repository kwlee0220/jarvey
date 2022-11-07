package jarvey.sample.perf;

import static jarvey.optor.geom.join.SpatialJoinOptions.ARC_CLIP;
import static jarvey.optor.geom.join.SpatialJoinOptions.SEMI_JOIN;

import java.util.concurrent.Callable;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;

import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTenMinutePolicy implements Callable<Void> {
	private static final String ELDERLY_CARE = "POI/노인복지시설";
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String POP_DENSITY = "주민/인구밀도_2000";
	private static final String HDONG = "구역/행정동코드";
	
	private final JarveySession m_jarvey;
	private final String m_outDsId;
	
	public BuildTenMinutePolicy(JarveySession jarvey, String outDsId) {
		m_jarvey = jarvey;
		m_outDsId = outDsId;
	}
	
	public Void call() throws Exception {
		// 노인복지시설에서 경로당 위치를 추출하여 버퍼 연산 수행
		SpatialDataFrame elderly = m_jarvey.read().dataset(ELDERLY_CARE)
										.filter("induty_nm = '경로당'")
										.select("the_geom")
										.buffer(400);
		
		// 인구수가 10000이상인 '인구밀도_2017' 지역 중심점 추출 수행
		SpatialDataFrame highDensity
				= m_jarvey.read().dataset(POP_DENSITY)
							.filter("value >= 10000")
							.select("the_geom")
							.centroid();

		// 인구수가 10000이상인 인구밀도 중심점을 포함한 행정동 위치 추출
		SpatialDataFrame highDensityHDong
				= m_jarvey.read().dataset(HDONG)
							.select("the_geom")
							.spatialBroadcastJoin(highDensity, SEMI_JOIN());
		
		SpatialDataFrame result
				= m_jarvey.read().dataset(CADASTRAL)
							// 지적 중에서 주변 400미터내에 경로당이 없는 지적만 추출
							.spatialBroadcastJoin(elderly, SEMI_JOIN().negated())
							// 결과 지적들 중에서 인구수 10000이상인 행정동 영역과 겹치는 부분 추출
							.spatialBroadcastJoin(highDensityHDong, ARC_CLIP())
							.repartition(7);
		
		result.writeSpatial().force(true).dataset(m_outDsId);
		
		return null;
	}

	private static final String RESULT = "tmp/10min/elderly_care_candidates";
	public static final void main(String... args) throws Exception {
		JarveySession jarvey = JarveySession.builder()
											.appName("10-min policy")
											.master("local[7]")
											.getOrCreate();
		
		StopWatch watch = StopWatch.start();
		BuildTenMinutePolicy app = new BuildTenMinutePolicy(jarvey, RESULT);
		app.call();
		watch.stop();
		
		SpatialDataFrame reloaded =jarvey.read().dataset(RESULT);
		System.out.printf("완료: '경로당필요지역' 추출, count=%d, elapsed=%s%n",
							reloaded.count(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		reloaded.drop("the_geom").show(5);
	}
}
