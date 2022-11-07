package jarvey.type.temporal.build;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterTemporalPointByTime {
	static final Logger s_logger = LoggerFactory.getLogger(ClusterTemporalPointByTime.class);
	
	public static final String PERIOD_COL_NAME = "period_id";

	private final JarveySession m_jarvey;
	private final String m_outputDsId;
	
	public ClusterTemporalPointByTime(JarveySession jarvey,
								String outputDsId) {
		m_jarvey = jarvey;
		m_outputDsId = outputDsId;
	}
	
	public void run(Dataset<Row> input) {
		Dataset<Row> rows;
		
		// 적절한 repartition() 함수 호출용 partition 갯수를 추정한다.
		// 필요한 partition 수의 1.7배를 사용한다.
		// 1.5배를 사용하는 이유는 하나의 task에 여러 partition이 배당되는 경우을 낮추기 위함.
		long nparts = input.select(PERIOD_COL_NAME)
							.distinct()
							.count();
		s_logger.info("# of distincts: {} -> partition count: {}", nparts, Math.round(nparts * 1.7));
		nparts = Math.round(nparts * 1.7);
		
		// PERIOD_COL_NAME을 기준으로 key기반 partition을 나눈다.
		rows = input.repartition((int)nparts, input.col(PERIOD_COL_NAME));
		
		m_jarvey.toSpatial(rows)
				.writeSpatial()
				.partitionBy(ClusterTemporalPointByTime.PERIOD_COL_NAME)
				.force(true)
				.dataset(m_outputDsId);
	}
}
