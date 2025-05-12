package jarvey.cluster;

import static jarvey.jarvey_functions.ST_BoxIntersectsBox;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.apache.spark.sql.SaveMode;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.cluster.EstimateQuadSpaces.PartitionEstimate;
import jarvey.support.MapTile;
import jarvey.support.RecordLite;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.StopWatch;
import utils.Tuple;
import utils.UnitUtils;
import utils.Utilities;
import utils.io.FilePath;
import utils.stream.FStream;

/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class ClusterDataset implements Callable<SpatialClusterFile> {
	private static final Logger s_logger = LoggerFactory.getLogger(ClusterDataset.class);

	public static final long OUTLIER_LIMIT = UnitUtils.parseByteSize("2mb");
	
	private final JarveySession m_jarvey;
	private final String m_dsId;
	private final ClusterDatasetOptions m_opts;
	
	public ClusterDataset(JarveySession jarvey, String dsId, ClusterDatasetOptions opts) {
		Utilities.checkArgument(opts.isValid(), "invalid ClusterDatasetOptions");
		
		m_jarvey = jarvey;
		m_dsId = dsId;
		m_opts = opts;
	}

	@Override
	public SpatialClusterFile call() throws IOException {
		StopWatch totalWatch = StopWatch.start();
		
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("start clustering...: id={}, opts=[{}]", m_dsId, m_opts);
		}
		
		SpatialClusterFile scFile = null;
		long[] quadIds = m_opts.candidateQuadIds();
		if ( quadIds != null ) {
			// quad-space partition 정보가 미리 지정된 경우에는 cluster를 저장하고 반환한다.
			scFile = writeClusters(m_dsId, quadIds);
		}
		else {
			// quad-space를 추정한다.
			quadIds = estimateQuadIds();
			// quad-id에 outlier quad-id가 포함되었으면 제거함.
			quadIds = FStream.of(quadIds).filter(id -> id != MapTile.OUTLIER_QID).mapToLong(v -> v).toArray();
			Set<Long> qidSet = Sets.newHashSet(Longs.asList(quadIds));
	
			scFile = writeClusters(m_dsId, quadIds);
			ClusterInfo cluster = ClusterInfo.from(scFile);
			
			// clustering 후 지정된 크기를 넘는 partition을 분할함.
			if ( m_opts.clusterSizeLimit().isPresent() ) {
				splitLargePartitions(cluster, scFile,  m_opts.clusterSizeLimit().get());
			}
			
			// 크기가 너무 작은 partition이 존재하는 경우 주변 partition들과 merge함.
			m_opts.clusterSizeLimit().ifPresentOrThrow(limit -> mergeQuadSpaces(cluster, limit));
			
			if ( m_opts.dropFinalOutliers() ) {
				dropOutlierPartitions(cluster, m_opts.outlierSizeLimit());
			}
			
			if ( !qidSet.equals(Sets.newHashSet(Longs.asList(cluster.quadIds()))) ) {
				scFile = writeClusters(m_dsId, cluster.quadIds());
			}
		}
		
		// QuadID set 파일 생성한다.
		ClusterInfo cluster = ClusterInfo.from(scFile);
		writeQuadSpaceIds(cluster);
			
		if ( s_logger.isInfoEnabled() ) {
			FilePath clusterPath = m_jarvey.getClusterFilePath(m_dsId);
			s_logger.info("clustered: id={}, path={}, opts={}, nclusters={}, elapsed={}",
							m_dsId, clusterPath, m_opts, scFile.getPartitionCount(),
							totalWatch.stopAndGetElpasedTimeString());
		}
		return scFile;
	}
	
	private long[] estimateQuadIds() {
		EstimateQuadSpacesOptions opts = m_opts.toEstimateQuadKeysOptions();
		
		StopWatch watch = StopWatch.start();
		
		EstimateQuadSpaces estimate = new EstimateQuadSpaces(m_jarvey, m_dsId, opts);
		Tuple<Integer, TreeSet<PartitionEstimate>> result = estimate.call();
		
		TreeSet<PartitionEstimate> clusters = result._2;
		long[] quadIds = FStream.from(clusters).mapToLong(PartitionEstimate::quadId).toArray();
		watch.stop();
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("QuadSpace estimation done: elapased={}, space_count={}",
							watch.getElapsedMillisString(), quadIds.length);
		}
		return quadIds;
	}
	
	private long[] mergeQuadSpaces(ClusterInfo cluster, long upperLimit) throws IOException {
		while ( cluster.partitions().size() > 1 ) {
			PartitionInfo merged = cluster.merge(upperLimit);
			if ( merged == null ) {
				break;
			}
		}
		return cluster.quadIds();
	}
	
	private void splitLargePartitions(ClusterInfo cluster, SpatialClusterFile scFile,
										long upperLimit) throws IOException {
		for ( SpatialPartitionFile spFile: scFile.streamPartitions(true) ) {
			PartitionInfo partInfo = cluster.getPartition(spFile.getQuadKey());
			if ( partInfo.m_length > upperLimit ) {
				splitPartition(cluster, spFile, upperLimit);
			}
		}
	}
	
//	private void splitPartition(ClusterInfo cluster, SpatialPartitionFile spFile) {
//		SpatialDataFrame sdf = spFile.read()
//									.select(SpatialDataFrame.COLUMN_ENVL4326)
//									.cache();
//		
//		String quadKey = spFile.getQuadKey();
//		PartitionInfo partInfo = cluster.getPartition(quadKey);
//		long avgRecLength = Math.round(partInfo.m_length / (double)partInfo.m_count);
//		cluster.removePartition(quadKey);
//		
//		MapTile[] subTiles = FStream.range(0, 4)
//									.mapToObj(v -> quadKey + v.toString())
//									.map(MapTile::fromQuadKey)
//									.toArray(MapTile.class);
//		List<PartitionInfo> splits = Lists.newArrayList();
//		for ( int i =0; i < 4; ++i ) {
//			Envelope bounds = subTiles[i].getBounds();
//			long count = sdf.filter(ST_BoxIntersectsBox(sdf.col(SpatialDataFrame.COLUMN_ENVL4326), bounds))
//							.count();
//			if ( count > 0 ) {
//				PartitionInfo split = new PartitionInfo(subTiles[i].getQuadId(), subTiles[i].getQuadKey(),
//															count, count*avgRecLength);
//				splits.add(split);
//				cluster.addPartition(split);
//			}
//		}
//		sdf.unpersist();
//		
//		if ( s_logger.isInfoEnabled() ) {
//			String splitsStr = FStream.from(splits)
//									.map(p -> String.format("%s(%s)", p.m_quadKey, UnitUtils.toByteSizeString(p.m_length)))
//									.join(", ");
//			String msg = String.format("splitted: %s(%s) -> [%s]",
//										quadKey, UnitUtils.toByteSizeString(partInfo.m_length), splitsStr);
//			s_logger.info(msg);
//		}
//	}
	
	private void dropOutlierPartitions(ClusterInfo cluster, long limit) {
		Logger logger = LoggerFactory.getLogger(s_logger.getName() + ".DROP");
		while ( true ) {
			PartitionInfo outlier = FStream.from(cluster.partitions())
											.filter(p -> p.m_length < limit)
											.sort((p1,p2) -> (int)(p1.m_length - p2.m_length))
											.findFirst()
											.getOrNull();
			if ( outlier == null ) {
				return;
			}
			
			cluster.removePartition(outlier.m_quadKey);
			if ( logger.isInfoEnabled() ) {
				logger.info("  drop tiny partition: {}", outlier);
			}
		}
	}
	
	private SpatialClusterFile writeClusters(String dsId, long[] quadIds) throws IOException {
		StopWatch watch = StopWatch.start();
		m_jarvey.read()
				.dataset(dsId)
				.cluster(dsId, quadIds, m_opts.force());

		FilePath clusterPath = m_jarvey.getClusterFilePath(dsId);
		SpatialClusterFile scFile = new SpatialClusterFile(m_jarvey, clusterPath);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("cluster-files are written: nclusters={}, elapsed: {}",
							scFile.getPartitionCount(), watch.stopAndGetElpasedTimeString());
		}
		return scFile;
	}

	private void writeQuadSpaceIds(ClusterInfo cluster) {
		JarveySchema jschema = JarveySchema.builder()
											.addJarveyColumn("qid", JarveyDataTypes.Long_Type)
											.build();
		List<RecordLite> idRecs = FStream.of(cluster.quadIds())
										.map(id -> RecordLite.of(new Object[]{id}))
										.toList();
		SpatialDataFrame sdf = m_jarvey.parallelize(idRecs, jschema);
		
		FilePath qidSetPath = m_jarvey.getQuadSetFilePath(m_dsId);
		sdf.toDataFrame().write().mode(SaveMode.Overwrite).csv(qidSetPath.getAbsolutePath());
	}
	
	private static class ClusterInfo {
		Map<String,PartitionInfo> m_partionInfos = Maps.newHashMap();
		
		static ClusterInfo from(SpatialClusterFile scFile) throws IOException {
			return scFile.streamPartitions(true)
						.map(p -> new PartitionInfo(p.getQuadId(), p.getQuadKey(), p.count(), p.getLength()))
						.collect(new ClusterInfo(), (c, p) -> c.addPartition(p));
		}
		
		long[] quadIds() {
			return FStream.from(partitions()).mapToLong(p -> p.m_quadId).toArray();
		}
		
		PartitionInfo getPartition(String quadKey) {
			return m_partionInfos.get(quadKey);
		}
		
		Collection<PartitionInfo> partitions() {
			return m_partionInfos.values();
		}
		
		void addPartition(PartitionInfo part) {
			m_partionInfos.put(part.m_quadKey, part);
		}
		
		void removePartition(String quadKey) {
			m_partionInfos.remove(quadKey);
		}
		
		PartitionInfo merge(long upperLimit) {
			Logger logger = LoggerFactory.getLogger(s_logger.getName() + ".MERGE");
			for ( PartitionInfo part: partitions() ) {
				List<PartitionInfo> siblings = findSiblings(part.m_quadKey);
				if ( siblings.size() == 4 ) {
					long totalSize = FStream.from(siblings).mapToLong(info -> info.m_length).sum();
					if ( totalSize <= upperLimit ) {
						// sibling partition를 모두 제거한다.
						FStream.from(siblings).map(p -> p.m_quadKey).forEach(k -> m_partionInfos.remove(k));
						
						// merge된 partition을 추가한다.
						String qk = siblings.get(0).m_quadKey;
						String prefix = qk.substring(0, qk.length()-1);
						long totalCount = FStream.from(siblings).mapToLong(info -> info.m_count).sum();
						PartitionInfo merged = new PartitionInfo(MapTile.toQuadId(prefix), prefix, totalCount, totalSize);
						addPartition(merged);
						
						if ( logger.isInfoEnabled() ) {
							String siblingStr
								= FStream.from(siblings)
										.sort((p1,p2) -> p1.m_quadKey.compareTo(p2.m_quadKey))
										.map(p -> String.format("%s(%s)", p.m_quadKey, p.getLengthString()))
										.join(",");
							String msg = String.format("  merge partitions: [%s] -> %s(%s): nparts=%d",
														siblingStr, prefix, UnitUtils.toByteSizeString(totalSize),
														m_partionInfos.size());
							logger.info(msg);
						}
						
						return merged;
					}
				}
			}
			
			return null;
		}
		
		private List<PartitionInfo> findSiblings(String qkey) {
			String prefix = qkey.substring(0, qkey.length()-1);
			return FStream.from(m_partionInfos.values())
							.filter(info -> info.m_quadKey.length() == qkey.length())
							.filter(info -> info.m_quadKey.startsWith(prefix))
							.toList();
		}
	}
	private static class PartitionInfo {
		private final long m_quadId;
		private final String m_quadKey;
		private final long m_length;
		private final long m_count;
		
		PartitionInfo(long quadId, String quadKey, long count, long length) {
			m_quadId = quadId;
			m_quadKey = quadKey;
			m_count = count;
			m_length = length;
		}
		
		String getLengthString() {
			return UnitUtils.toByteSizeString(m_length);
		}
		
		@Override
		public String toString() {
			return String.format("%s(%d): count=%d, length=%s", m_quadKey, m_quadId, m_count,
								UnitUtils.toByteSizeString(m_length));
		}
	}
	
	private void splitPartition(ClusterInfo cluster, SpatialPartitionFile spFile, long limit) {
		SpatialDataFrame sdf = spFile.read()
									.select(SpatialDataFrame.COLUMN_ENVL4326)
									.cache();
		
		String quadKey = spFile.getQuadKey();
		PartitionInfo partInfo = cluster.getPartition(quadKey);
		long avgRecLength = Math.round(partInfo.m_length / (double)partInfo.m_count);
		cluster.removePartition(quadKey);
		
		List<PartitionInfo> splits = Lists.newArrayList();
		List<PartitionInfo> remains = Lists.newArrayList(partInfo);
		while ( remains.size() > 0 ) {
			PartitionInfo target = remains.remove(0);
			MapTile[] subTiles = FStream.range(0, 4)
										.mapToObj(v -> target.m_quadKey + v.toString())
										.map(MapTile::fromQuadKey)
										.toArray(MapTile.class);
			for ( int i =0; i < 4; ++i ) {
				Envelope bounds = subTiles[i].getBounds();
				long count = sdf.filter(ST_BoxIntersectsBox(sdf.col(SpatialDataFrame.COLUMN_ENVL4326), bounds)).count();
				if ( count > 0 ) {
					PartitionInfo split = new PartitionInfo(subTiles[i].getQuadId(), subTiles[i].getQuadKey(),
																count, count*avgRecLength);
					if ( split.m_length < limit ) {
						splits.add(split);
					}
					else {
						remains.add(split);
					}
				}
			}
		}
		sdf.unpersist();
		
		FStream.from(splits).forEach(cluster::addPartition);
		if ( s_logger.isInfoEnabled() ) {
			String splitsStr = FStream.from(splits)
									.map(p -> String.format("%s(%s)", p.m_quadKey, UnitUtils.toByteSizeString(p.m_length)))
									.join(", ");
			String msg = String.format("splitted: %s(%s) -> [%s]",
										quadKey, UnitUtils.toByteSizeString(partInfo.m_length), splitsStr);
			s_logger.info(msg);
		}
	}
}
