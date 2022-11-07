package jarvey.type.temporal.build;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import jarvey.support.Rows;
import jarvey.type.temporal.TemporalPoint;
import jarvey.type.temporal.TimedPoint;

import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class BuildInPartitionTPoint implements MapPartitionsFunction<Row, Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(BuildInPartitionTPoint.class);
	
	private final int m_keyCount;
	private final int m_xtyColIdx;
	private final int m_outColCount;
	private final Map<GroupKey,Set<TimedPoint>> m_groups = Maps.newHashMap();
	
	BuildInPartitionTPoint(int keyCount) {
		m_keyCount = keyCount;
		m_xtyColIdx = keyCount;
		m_outColCount = keyCount + 1;
	}

	@Override
	public Iterator<Row> call(Iterator<Row> rows) throws Exception {
		StopWatch watch = StopWatch.start();
		
		FStream.from(rows).forEach(this::collect);

		int partId = TaskContext.getPartitionId();
		return FStream.from(m_groups)
					.map((key, pts) -> {
						Object[] values = Arrays.copyOf(key.m_key, m_outColCount);
						
						TemporalPoint tpoint = new TemporalPoint(Lists.newArrayList(pts));
						Row tpRow = tpoint.toRow();
						values[m_xtyColIdx] = tpRow;
						
						return RowFactory.create(values);
					})
					.onClose(() -> {
						if ( s_logger.isInfoEnabled() ) {
							String msg = String.format("[%4d] %8s: in-partition TemporalPoints: count=%d, elapsed=%s",
														partId, "built", m_groups.size(),
														watch.stopAndGetElpasedTimeString());
							s_logger.info(msg);
						}
					})
					.iterator();
	}

	private void collect(Row row) {
		GroupKey groupKey = toGroupKey(row);
		TimedPoint tp = new TimedPoint(row.getFloat(m_xtyColIdx), row.getFloat(m_xtyColIdx+1),
										row.getLong(m_xtyColIdx+2));
		Set<TimedPoint> group = m_groups.computeIfAbsent(groupKey, k -> new TreeSet<>());
		group.add(tp);
	}
	
	private GroupKey toGroupKey(Row row) {
		return new GroupKey(Rows.toArray(row, m_keyCount, m_keyCount));
	}
	
	private static final class GroupKey {
		private final Object[] m_key;
		
		private GroupKey(Object[] key) {
			m_key = key;
		}
		
		@Override
		public String toString() {
			return FStream.of(m_key).join(':');
		}
		
		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			else if ( obj == null || obj.getClass() != GroupKey.class ) {
				return false;
			}
			
			GroupKey other = (GroupKey)obj;
			return Arrays.equals(m_key, other.m_key);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(m_key);
		}
	}
}