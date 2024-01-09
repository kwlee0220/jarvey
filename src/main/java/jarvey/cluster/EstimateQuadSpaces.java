package jarvey.cluster;

import static jarvey.jarvey_functions.JV_IsValidEnvelope;
import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.cluster.EstimateQuadSpaces.PartitionEstimate;
import jarvey.support.MapTile;
import jarvey.support.RecordLite;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.Tuple;
import utils.stream.FStream;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class EstimateQuadSpaces implements Callable<Tuple<Integer, TreeSet<PartitionEstimate>>> {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusterDataset.class);
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final EstimateQuadSpacesOptions m_opts;
	
	private static final String ENVL4326 = SpatialDataFrame.COLUMN_ENVL4326;
	private static final int RECORD_OVERHEAD_SIZE = 4 * 8 	// 4 long for Envelope
													+ 1 * 8	// 1 long for primary cluster-id
													+ 1 * 8 + 4; // 1 long for cluster-id array + array length
	
	public EstimateQuadSpaces(JarveySession jarvey, String dsId, EstimateQuadSpacesOptions opts) {
		Utilities.checkArgument(opts.isValid(), "invalid EstimateQuadSpacesOptions");
		
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_opts = opts;
	}

	@Override
	public Tuple<Integer, TreeSet<PartitionEstimate>> call() {
		StopWatch watch = StopWatch.start();
		
		SpatialDataFrame sdf = m_jarvey.read().dataset(m_dsId);

		// Sampling된 bounding-box 정보를 driver에 복사함
		List<RecordLite> samples
			= sdf.sample(m_opts.sampleRatio())				// 샘플 채취
				.box2d(ENVL4326)							// Boundingbox를 추출하여 시스템 정의 컬럼 값에 기록
				.transform_box(ENVL4326, 4326)				// SRID를 위경도 좌표계를 변환.
				.filter(col(ENVL4326).isNotNull())			// null인 경우를 제외
				.filter(JV_IsValidEnvelope(col(ENVL4326)))	// invalid한 boundingbox를 제외
				.select(ENVL4326)
				.collectAsRecordList();
		if ( s_logger.isInfoEnabled() ) {
			String msg = String.format("sampled: ratio=%.2f, count=%d, elapsed=%s",
										m_opts.sampleRatio(), samples.size(), watch.stopAndGetElpasedTimeString());
			s_logger.info(msg);
		}
		
		// 레코드의 평균 크기를 추정함.
		long dsSize = m_jarvey.calcDatasetLength(m_dsId);
		double totalCountEst = samples.size() / m_opts.sampleRatio();	// Dataset에 포함된 전체 레코드 수 추산
		double adjRecordSizeEst = ((dsSize / totalCountEst) + RECORD_OVERHEAD_SIZE) / m_opts.sampleRatio();
		
		// 초기 partition 생성
		PartitionEstimate root = new PartitionEstimate("", dsSize, adjRecordSizeEst);
		samples.forEach(rec -> root.add(rec.getEnvelope(0)));
		
		// Partition set 추정 수행
		long sizeHint = m_opts.clusterSizeHint();
		long upperLimit = m_opts.clusterSizeLimit()
								.map(limit -> sizeHint + Math.round((limit - sizeHint) * 0.7))
								.getOrElse(Math.round(sizeHint * 1.2));
		TreeSet<PartitionEstimate> quadSpaces = estimateClusters(root, upperLimit);
		
		PartitionEstimate outlier = new PartitionEstimate(MapTile.OUTLIER_QKEY, dsSize, root.m_adjRecordLengthEst);
		quadSpaces = FStream.from(quadSpaces.descendingIterator())
							.filter(est -> {
								if ( est.quadId() == MapTile.OUTLIER_QID ) {
									FStream.from(est.m_boxes).forEach(outlier::add);
									return false;
								}
								if ( est.sizeEstimate() < m_opts.outlierSizeLimit() ) {
									if ( s_logger.isInfoEnabled() ) {
										String msg = String.format("drop sample outlier: %s: %s, count=%d",
																	est.m_quadKey, UnitUtils.toByteSizeString(est.sizeEstimate()),
																	est.m_boxes.size());
										s_logger.info(msg);
									}
									FStream.from(est.m_boxes).forEach(outlier::add);
									return false;
								}
								else {
									return true;
								}
							})
							.collect(new TreeSet<>(), (accum, est) -> accum.add(est));
		if ( outlier.recordCount() > 0 ) {
			quadSpaces.add(outlier);
		}
		
		if ( m_opts.save() ) {
			saveQuadSpaceIds(quadSpaces);
		}
		
		if ( s_logger.isInfoEnabled() ) {
			String msg = String.format("estimate quad-spaces: %s opts=[%s], space_count=%d, elapsed=%s",
										m_dsId, m_opts, quadSpaces.size(), watch.stopAndGetElpasedTimeString());
			s_logger.info(msg);
		}
		
		return Tuple.of(root.recordCount(), quadSpaces);
	}
	
	private TreeSet<PartitionEstimate> estimateClusters(PartitionEstimate root, long limit) {
		TreeSet<PartitionEstimate> clusters = new TreeSet<>();
		clusters.add(root);
		
		while ( clusters.first().sizeEstimate() >= limit ) {
			PartitionEstimate cluster = clusters.pollFirst();
			for ( PartitionEstimate split: cluster.split() ) {
				if ( split.recordCount() > 0 ) {
					clusters.add(split);
				}
			}
		}
		
		return clusters;
	}
	
	private void saveQuadSpaceIds(TreeSet<PartitionEstimate> quadSpaces) {
		JarveySchema jschema = JarveySchema.builder()
											.addJarveyColumn("qid", JarveyDataTypes.Long_Type)
											.build();
		List<RecordLite> qidRecords = FStream.from(quadSpaces)
											.map(sp -> RecordLite.of(new Object[]{sp.quadId()}))
											.toList();
		Dataset<Row> qidDf = m_jarvey.parallelize(qidRecords, jschema).toDataFrame();
		String qidSetPath = m_jarvey.getQuadSetFilePath(m_dsId).getAbsolutePath();
		qidDf.write().mode(SaveMode.Overwrite).csv(qidSetPath);
	}

	public static class PartitionEstimate implements Comparable<PartitionEstimate> {
		private final String m_quadKey;
		private final long m_totalLength;
		private final double m_adjRecordLengthEst;
		private final List<Envelope> m_boxes = Lists.newArrayList();
		
		PartitionEstimate(String quadKey, long totalLength, double adjRecordLength) {
			m_quadKey = quadKey;
			m_totalLength = totalLength;
			m_adjRecordLengthEst = adjRecordLength;
		}
		
		public String quadKey() {
			return m_quadKey;
		}
		
		public long quadId() {
			return MapTile.toQuadId(m_quadKey);
		}
		
		public int recordCount() {
			return m_boxes.size();
		}
		
		public long sizeEstimate() {
			return Math.round(m_boxes.size() * m_adjRecordLengthEst);
		}
		
		public double portion() {
			return (double)sizeEstimate() / m_totalLength;
		}
		
		void add(Envelope box) {
			m_boxes.add(box);
		}
		
		@Override
		public String toString() {
			return String.format("%s(%d): size=%s, count=%d, portion=%.2f%%", m_quadKey, quadId(),
													UnitUtils.toByteSizeString(sizeEstimate()),
													recordCount(), portion()*100);
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			else if ( obj == null  || obj.getClass() != PartitionEstimate.class ) {
				return false;
			}
			
			PartitionEstimate other = (PartitionEstimate)obj;
			return m_quadKey.equals(other.m_quadKey);
		}

		@Override
		public int compareTo(PartitionEstimate o) {
			return o.m_boxes.size() - m_boxes.size();
		}
		
		private List<PartitionEstimate> split() {
			List<PartitionEstimate> splits = Lists.newArrayList();
			MapTile[] tiles = FStream.range(0, 4)
										.mapToObj(v -> m_quadKey + v.toString())
										.peek(qkey -> splits.add(new PartitionEstimate(qkey, m_totalLength,
																						m_adjRecordLengthEst)))
										.map(qkey -> MapTile.fromQuadKey(qkey))
										.toArray(MapTile.class);
			for ( Envelope envl: m_boxes ) {
				for ( int i =0; i < 4; ++i ) {
					if ( tiles[i].contains(envl) ) {
						splits.get(i).add(envl);
					}
				}
			}
			
			return splits;
		}
	};
}
