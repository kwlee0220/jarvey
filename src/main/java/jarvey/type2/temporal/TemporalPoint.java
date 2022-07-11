/**
 * 
 */
package jarvey.type2.temporal;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import com.clearspring.analytics.util.Lists;

import jarvey.type.GeometryValue;
import scala.collection.mutable.WrappedArray;
import utils.func.Lazy;
import utils.geo.util.GeoClientUtils;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalPoint implements Row {
	private static final long serialVersionUID = 1L;

	private Lazy<TimedPoint[]> m_timeds;
	private WrappedArray<Row> m_wrapped;
	
	private static final TimeComparator COMTOR = new TimeComparator();
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("points", DataTypes.createArrayType(TimedPoint.DATA_TYPE), false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public TemporalPoint() {
		this(new TimedPoint[0]);
	}
	
	public TemporalPoint(TimedPoint[] timeds) {
		m_timeds = Lazy.of(timeds);
		m_wrapped = null;
	}
	
	public TemporalPoint(WrappedArray<Row> timedsArray) {
		m_timeds = Lazy.of(() -> TemporalPoint.toTimedPointArray(timedsArray));
		m_wrapped = timedsArray;
	}
	
	public int getNumPoints() {
		return m_timeds.isLoaded() ? m_timeds.get().length : m_wrapped.length();
	}
	
	public TimedPoint[] getTimedPoints() {
		return m_timeds.get();
	}
	
	public TimedPoint getTimedPoint(int idx) {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[idx];
		}
		else {
			return TimedPoint.fromRow(m_wrapped.apply(idx));
		}
	}
	
	public TimedPoint getFirstTimedPoint() {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[0];
		}
		else {
			return TimedPoint.fromRow(m_wrapped.head());
		}
	}
	
	public TimedPoint getLastTimedPoint() {
		if ( m_timeds.isLoaded() ) {
			int npoints = getNumPoints();
			return m_timeds.get()[npoints-1];
		}
		else {
			return TimedPoint.fromRow(m_wrapped.last());
		}
	}
	
	public GeometryValue getFirstPoint() {
		return getFirstTimedPoint().getGeometryRow();
	}
	
	public GeometryValue getLastPoint() {
		return getFirstTimedPoint().getGeometryRow();
	}
	
	public Timestamp getFirstTimestamp() {
		if ( m_timeds.isLoaded() ) {
			return new Timestamp(m_timeds.get()[0].getTs());
		}
		else {
			return new Timestamp(m_wrapped.head().getLong(1));
		}
	}
	
	public Timestamp getLastTimestamp() {
		if ( m_timeds.isLoaded() ) {
			int npoints = getNumPoints();
			return new Timestamp(m_timeds.get()[npoints-1].getTs());
		}
		else {
			return new Timestamp(m_wrapped.last().getLong(1));
		}
	}
	
	public long getDuration() {
		int npoints = getNumPoints();
		if ( npoints > 1 ) {
			return getTimedPoint(npoints-1).getTs() - getTimedPoint(0).getTs();
		}
		else {
			return 0;
		}
		
	}
	
	public LineString getPath() {
		TimedPoint[] timeds = m_timeds.get();
		
		if ( timeds.length > 1 ) {
			List<Coordinate> coords = FStream.of(m_timeds.get())
											.map(TimedPoint::getPoint)
											.map(Point::getCoordinate)
											.toList();
			return GeometryUtils.toLineString(coords);
		}
		else if ( timeds.length == 1 ) {
			Coordinate coord = timeds[0].getPoint().getCoordinate();
			return GeometryUtils.toLineString(coord, coord);
		}
		else {
			return GeoClientUtils.EMPTY_LINESTRING;
		}
	}
	
	public TemporalPoint sliceByTime(long beginMillis, long endMillis) {
		TimedPoint[] pts = m_timeds.get();
		TimedPoint[] slice = FStream.of(pts)
									.dropWhile(tp -> tp.getTs() < beginMillis)
									.takeWhile(tp -> tp.getTs() < endMillis)
									.toArray(TimedPoint.class);
		return new TemporalPoint(slice);
	}
	
	public void add(TimedPoint tpt) {
		TimedPoint[] timeds = m_timeds.get();
		int idx = Arrays.binarySearch(timeds, tpt, COMTOR);
		idx = (idx >= 0) ? idx : -(idx + 1);
		
		TimedPoint[] added = new TimedPoint[timeds.length+1];
		if ( idx > 0 ) {
			System.arraycopy(timeds, 0, added, 0, idx);
		}
		added[idx] = tpt;
		
		int remains = timeds.length - idx;
		if ( remains > 0 ) {
			System.arraycopy(timeds, idx, added, idx+1, remains);
		}
		
		m_timeds = Lazy.of(added);
	}
	
	public static final TemporalPoint merge(TemporalPoint tp1, TemporalPoint tp2) {
		int idx1 = 0;
		int idx2 = 0;
		
		TimedPoint[] timeds1 = tp1.getTimedPoints();
		TimedPoint[] timeds2 = tp2.getTimedPoints();
		List<TimedPoint> mergeds = Lists.newArrayList();
		while ( idx1 < timeds1.length && idx2 < timeds2.length ) {
			TimedPoint pt1 = timeds1[idx1];
			TimedPoint pt2 = timeds2[idx2];
			int cmp = COMTOR.compare(pt2, pt1);
			if ( cmp <= 0 ) {
				mergeds.add(pt1);
				++idx1;
			}
			else {
				mergeds.add(pt2);
				++idx2;
			}
		}
		
		mergeds.addAll(Arrays.asList(timeds1).subList(idx1, timeds1.length));
		mergeds.addAll(Arrays.asList(timeds2).subList(idx2, timeds2.length));
		
		TimedPoint[] tps = mergeds.toArray(new TimedPoint[mergeds.size()]);
		return new TemporalPoint(tps);
	}
	
	public static TemporalPoint fromRow(Row row) {
		if ( row instanceof TemporalPoint ) {
			return (TemporalPoint)row;
		}
		else {
			return new TemporalPoint((WrappedArray)row.getAs(0));
		}
	}

	@Override
	public Row copy() {
		return m_timeds.isLoaded() ? new TemporalPoint(m_timeds.get()) : new TemporalPoint(m_wrapped);
	}

	@Override
	public Object get(int i) {
		if ( i == 0 ) {
			return m_timeds.get();
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public int length() {
		return 1;
	}
	
	@Override
	public String toString() {
		int npoints = getNumPoints();
		if ( npoints > 1) {
			TimedPoint first = getTimedPoint(0);
			TimedPoint last = getTimedPoint(npoints-1);
			
			long millis = last.getTs() - first.getTs();
			long seconds = Duration.ofMillis(millis).getSeconds();
			String durStr = String.format("%02d:%02d:%02d", seconds/3600, (seconds%3600)/60, seconds%60);
			
			return String.format("%s[%d][%s:%s]", durStr, npoints, first, last);
		}
		else if ( npoints == 1 ) {
			TimedPoint first = getTimedPoint(0);
			return String.format("[%d][%s]", npoints, first);
		}
		else {
			return "empty";
		}
	}
	
	private static final class TimeComparator implements Comparator<TimedPoint> {
		@Override
		public int compare(TimedPoint o1, TimedPoint o2) {
			return Long.compare(o1.getTs(), o2.getTs());
		}
	}
	
	public static TimedPoint[] toTimedPointArray(WrappedArray warray) {
		TimedPoint[] timeds = new TimedPoint[warray.length()];
		for ( int i =0; i < timeds.length; ++i ) {
			Row timedRow = (Row)warray.apply(i);
			timeds[i] = TimedPoint.fromRow(timedRow);
		}
		
		return timeds;
	}
}
