package jarvey.cluster;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.Partition;
import org.apache.spark.Partitioner;

import com.google.common.collect.Sets;

import utils.Utilities;
import utils.stream.FStream;

import jarvey.support.MapTile;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QuadSpacePartitioner extends Partitioner {
	private static final long serialVersionUID = 1L;
	
	private final long[] m_quadIds;
	private final QuadSpacePartition[] m_partitions;
	
	public static QuadSpacePartitioner from(Iterable<Long> quadIds) {
		Set<Long> qids = Sets.newTreeSet(quadIds);
		qids.add(MapTile.OUTLIER_QID);	// // qids에 outlier qid가 포함되지 않은 경우를 대비

		// qid를 기준으로 sort하기 때문에 outlier quid가 0번째 위치하게 됨.
		QuadSpacePartition[] partitions = FStream.from(qids)
												.zipWithIndex()
												.map(t -> new QuadSpacePartition(t.index(), t.value()))
												.toArray(QuadSpacePartition.class);
		Utilities.checkArgument(partitions.length > 0, "empty quadkeys");
		
		return new QuadSpacePartitioner(partitions);
	}
	
	private QuadSpacePartitioner(QuadSpacePartition[] partitions) {
		m_partitions = partitions;
		m_quadIds = FStream.of(partitions).mapToLong(QuadSpacePartition::getQuadId).toArray();
	}
	
	public long[] getQuadIds() {
		return m_quadIds;
	}
	
	public QuadSpacePartition[] getPartitionAll() {
		return m_partitions;
	}
	
	public QuadSpacePartition getPartition(int idx) {
		return m_partitions[idx];
	}

	@Override
	public int getPartition(Object key) {
		int ret = Arrays.binarySearch(m_quadIds, (long)key);
		// 입력 key에 해당하는 quadid가 없는 경우에는 outlier (-9)에 해당하는 partition을 반환.
		return (ret >= 0) ? ret : 0;
	}

	@Override
	public int numPartitions() {
		return m_quadIds.length;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof QuadSpacePartitioner) ) {
			return false;
		}
		
		QuadSpacePartitioner other = (QuadSpacePartitioner)obj;
		return Objects.deepEquals(m_quadIds, other.m_quadIds);
	}
	
	@Override
	public int hashCode() {
		return m_quadIds.hashCode();
	}
	
	public static class QuadSpacePartition implements Partition {
		private static final long serialVersionUID = 1L;
		
		private final long m_quadId;
		private final int m_index;
		
		public QuadSpacePartition(int index, long quadId) {
			m_index = index;
			m_quadId = quadId;
		}
		
		public long getQuadId() {
			return m_quadId;
		}
		
		@Override
		public int index() {
			return m_index;
		}
		
		@Override
		public String toString() {
			return "" + m_index + ":" + m_quadId;
		}
	}
}
