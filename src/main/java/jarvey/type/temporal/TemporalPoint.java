package jarvey.type.temporal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import utils.Indexed;
import utils.UnitUtils;
import utils.io.IOUtils;
import utils.stream.FStream;

import jarvey.datasource.DatasetOperationException;
import jarvey.support.GeoUtils;
import jarvey.support.MapTile;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalPoint implements Comparable<TemporalPoint> {
	private static final Logger s_logger = LoggerFactory.getLogger(TemporalPoint.class);
	
	private final List<TimedPoint> m_points;
	
	public TemporalPoint(TimedPoint... timeds) {
		m_points = Arrays.asList(timeds);
	}
	
	public TemporalPoint(List<TimedPoint> timeds) {
		m_points = timeds;
	}
	
	public int length() {
		return m_points.size();
	}
	
	public List<TimedPoint> getSamples() {
		return m_points;
	}
	
	public TimedPoint getSample(int idx) {
		return m_points.get(idx);
	}
	
	public TimedPoint getFirstSample() {
		return m_points.get(0);
	}
	
	public TimedPoint getLastSample() {
		return m_points.get(m_points.size()-1);
	}
	
	public long getDurationMillis() {
		if ( m_points.size() > 1 ) {
			return getLastSample().getT() - getFirstSample().getT();
		}
		else {
			return 0;
		}
	}
	
	public Duration getDuration() {
		return Duration.of(getDurationMillis(), ChronoUnit.MILLIS);
	}
	
	public LineString toLineString() {
		return GeoUtils.toLineString(FStream.from(m_points)
											.map(TimedPoint::toCoordinate)
											.toList());
	}
	
	public void add(TimedPoint timed) {
		if ( m_points.size() == 0 ) {
			m_points.add(timed);
		}
		else {
			if ( timed.getT() >= getLastSample().getT() ) {
				m_points.add(timed);
			}
			else {
				int idx = Collections.binarySearch(m_points, timed);
				idx = (idx >= 0) ? idx : -(idx + 1);
				m_points.add(idx, timed);
			}
		}
	}
	
	public List<MapTile> getContainingMapTiles(List<MapTile> tiles) {
		Coordinate[] coords = FStream.from(m_points)
										.map(TimedPoint::toCoordinate)
										.toArray(Coordinate.class);
		return FStream.from(tiles)
						.filter(tile -> {
							boolean found = false;
							for ( int i =0; i < coords.length; ++i ) {
								if ( coords[i] != null && tile.contains(coords[i]) ) {
									found = true;
									coords[i] = null;
								}
							}
							return found;
						})
						.toList();
	}
	
	public List<TemporalPoint> splitByInterval(long maxIdleSeconds) {
		List<Integer> lastIndexes
						= FStream.from(m_points)
								.zipWithIndex()
								.buffer(2, 1)
								.filter(pair -> pair.size() > 1)
								.filter(pair -> {
									Indexed<TimedPoint> tup1 = pair.get(0);
									Indexed<TimedPoint> tup2 = pair.get(1);
									long delta = tup2.value().getT() - tup1.value().getT();
									return delta > maxIdleSeconds;
								})
								.map(pair -> pair.get(0).index())
								.toList();
		
		if ( lastIndexes.size() == 0 ) {
			return Arrays.asList(this);
		}
		else {
			List<TemporalPoint> sub = Lists.newArrayList();
			int startIdx = 0;
			for ( int lastIdx: lastIndexes ) {
				sub.add(new TemporalPoint(m_points.subList(startIdx, lastIdx)));
				startIdx = lastIdx + 1;
			}
			sub.add(new TemporalPoint(m_points.subList(startIdx, m_points.size()-1)));
			
			if ( s_logger.isInfoEnabled() ) {
				String idleDurStrs = FStream.from(lastIndexes)
											.map(idx -> m_points.get(idx+1).getT() -  m_points.get(idx).getT())
											.map(gap -> UnitUtils.toSecondString(gap))
											.join(",");
				

				String childrenStr = FStream.from(sub)
											.map(TemporalPoint::toString)
											.join(", ");
				s_logger.info("broken into {} splits [{}]: {} -> {}", sub.size(), idleDurStrs, this, childrenStr);
			}
			
			return sub;
		}
	}

	public static final TemporalPoint from(Row row) {
		return fromCompressedBytes(row.getAs(3));
	}
	
	public Row toRow() {
		try {
			byte[] compressed = toCompressedBytes();
			
			Timestamp firstTs = new Timestamp(getFirstSample().getT());
			Timestamp lastTs = new Timestamp(getLastSample().getT());
			return RowFactory.create(m_points.size(), firstTs, lastTs, compressed);
		}
		catch ( IOException e ) {
			throw new DatasetOperationException(e);
		}
	}
	
	public static TemporalPoint fromCompressedBytes(byte[] bytes) {
		try {
			ByteBuffer buf = ByteBuffer.wrap(IOUtils.decompress(bytes));
			
			int length = buf.getInt();
			long firstTs = buf.getLong();
			long lastTs = buf.getLong();

			List<TimedPoint> ptList = Lists.newArrayListWithCapacity(length);
			for ( int i =0; i < buf.remaining(); ++i ) {
				float x = buf.getFloat();
				float y = buf.getFloat();
				int offset = buf.getInt();
				
				ptList.add(new TimedPoint(x, y, firstTs + offset));
			}
			return new TemporalPoint(ptList);
		}
		catch ( Exception e ) {
			throw new DatasetOperationException(e);
		}
	}
	
	public byte[] toCompressedBytes() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(4+8+8 + (length() * 4 * 3));
		
		buf.putInt(m_points.size());
		long startTs = getFirstSample().getT();
		buf.putLong(startTs);
		buf.putLong(getLastSample().getT());
		
		for ( TimedPoint tpt: m_points ) {
			buf.putFloat(tpt.getX());
			buf.putFloat(tpt.getY());
			
			long offset = tpt.getT() - startTs;
			buf.putInt((int)offset);
		}
		return IOUtils.compress(buf.array());
	}
	
	public static TemporalPoint merge(TemporalPoint tp1, TemporalPoint tp2) {
		Iterable<TimedPoint> merged = Iterables.mergeSorted(Arrays.asList(tp1.m_points, tp2.m_points),
															TimedPoint.COMPTOR);
		return new TemporalPoint( Lists.newArrayList(merged));
		
		
		
//		List<TimedPoint> merged = Lists.newArrayList();
//		int idx1 = 0;
//		int idx2 = 0;
//		TimedPoint tpt1 = tp1.getSample(idx1);
//		TimedPoint tpt2 = tp2.getSample(idx2);
//		while ( tpt1 != null && tpt2 != null ) {
//			if ( tpt1.getT() < tpt2.getT() ) {
//				merged.add(tpt1);
//				tpt1 = (++idx1 < tp1.length()) ? tp1.getSample(idx1) : null;
//			}
//			else if ( tpt1.getT() > tpt2.getT() ) {
//				merged.add(tpt2);
//				tpt2 = (++idx2 < tp2.length()) ? tp2.getSample(idx2) : null;
//			}
//			else {
//				// 두개의 timed-point가 동일한 경우는 한쪽의 데이터 (tpt2)를 무시한다.
//				tpt2 = (++idx2 < tp2.length()) ? tp2.getSample(idx2) : null;
//			}
//		}
//		
//		if ( tpt1 != null ) {
//			for ( int i =idx1; i < tp1.length(); ++i ) {
//				merged.add(tp1.getSample(i));
//			}
//		}
//		else if ( tpt2 != null ) {
//			for ( int i =idx2; i < tp2.length(); ++i ) {
//				merged.add(tp2.getSample(i));
//			}
//		}
//		
//		return new TemporalPoint(merged);
	}
	
	public static TemporalPoint merge(List<TemporalPoint> tpList) {
		List<List<TimedPoint>> inputs = FStream.from(tpList)
												.map(tp -> tp.m_points)
												.toList();
		Iterable<TimedPoint> merged = Iterables.mergeSorted(inputs, TimedPoint.COMPTOR);
		return new TemporalPoint(Lists.newArrayList(merged));
		
//		tpList = Lists.newArrayList(tpList);
//		Collections.sort(tpList);
//		
//		List<TimedPoint> merged = Lists.newArrayList();
//		while ( tpList.size() >= 2 ) {
//			TemporalPoint tp1 = tpList.get(0);
//			TemporalPoint tp2 = tpList.get(1);
//			if ( tp1.getLastSample().getT() < tp2.getFirstSample().getT() ) {
//				merged.addAll(tp1.m_points);
//				tpList.remove(0);
//			}
//			else {
//				TemporalPoint tp3 = merge(tp1, tp2);
//				tpList.remove(0);
//				tpList.remove(0);
//				tpList.add(0, tp3);
//			}
//		}
//		merged.addAll(tpList.get(0).m_points);
//		
//		return new TemporalPoint(merged);
	}
	
	@Override
	public String toString() {
		int npoints = length();
		String durStr = UnitUtils.toSecondString(getDurationMillis());
		if ( npoints > 1) {
			return String.format("%s[%d]{%s - %s}", durStr, npoints, getFirstSample(), getLastSample());
		}
		else if ( npoints == 1 ) {
			TimedPoint first = getSample(0);
			return String.format("%s[%d][%s]", durStr, npoints, first);
		}
		else {
			return "empty";
		}
	}
	
	public String toShortString() {
		return String.format("%s[%d]", getDuration(), length());
	}

	@Override
	public int compareTo(TemporalPoint o) {
		return Long.compare(getFirstSample().getT(), o.getFirstSample().getT());
	}
	
	public static Comparator<TemporalPoint> COMPTOR = new Comparator<TemporalPoint>() {
		@Override
		public int compare(TemporalPoint o1, TemporalPoint o2) {
			return Long.compare(o1.getFirstSample().getT(), o2.getFirstSample().getT());
		}
	};
}
