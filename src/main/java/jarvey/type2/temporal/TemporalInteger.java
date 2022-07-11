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

import com.clearspring.analytics.util.Lists;

import scala.collection.mutable.WrappedArray;
import utils.func.Lazy;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalInteger implements Row {
	private static final long serialVersionUID = 1L;

	private Lazy<TimedInteger[]> m_timeds;
	private WrappedArray<Row> m_wrapped;
	
	private static final TimeComparator COMTOR = new TimeComparator();
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("samples", DataTypes.createArrayType(TimedInteger.DATA_TYPE), false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public TemporalInteger() {
		this(new TimedInteger[0]);
	}
	
	public TemporalInteger(TimedInteger[] timeds) {
		m_timeds = Lazy.of(timeds);
		m_wrapped = null;
	}
	
	public TemporalInteger(WrappedArray<Row> timedsArray) {
		m_timeds = Lazy.of(() -> TemporalInteger.toTimedIntegerArray(timedsArray));
		m_wrapped = timedsArray;
	}
	
	public int getNumSamples() {
		return m_timeds.isLoaded() ? m_timeds.get().length : m_wrapped.length();
	}
	
	public TimedInteger[] getSamples() {
		return m_timeds.get();
	}
	
	public TimedInteger getSampleAt(int idx) {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[idx];
		}
		else {
			return TimedInteger.fromRow(m_wrapped.apply(idx));
		}
	}
	
	public TimedInteger getFirstSample() {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[0];
		}
		else {
			return TimedInteger.fromRow(m_wrapped.head());
		}
	}
	
	public TimedInteger getLastSample() {
		if ( m_timeds.isLoaded() ) {
			int npoints = getNumSamples();
			return m_timeds.get()[npoints-1];
		}
		else {
			return TimedInteger.fromRow(m_wrapped.last());
		}
	}
	
	public int getFirstValue() {
		return getFirstSample().getValue();
	}
	
	public int getLastValue() {
		return getLastSample().getValue();
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
			int npoints = getNumSamples();
			return new Timestamp(m_timeds.get()[npoints-1].getTs());
		}
		else {
			return new Timestamp(m_wrapped.last().getLong(1));
		}
	}
	
	public long getDuration() {
		int npoints = getNumSamples();
		if ( npoints > 1 ) {
			return getLastSample().getTs() - getFirstSample().getTs();
		}
		else {
			return 0;
		}
		
	}
	
	public int[] getValues() {
		TimedInteger[] timeds = m_timeds.get();
		
		if ( timeds.length > 1 ) {
			return FStream.of(m_timeds.get())
							.mapToInt(TimedInteger::getValue)
							.toArray();
		}
		else if ( timeds.length == 1 ) {
			return new int[]{timeds[0].getValue()};
		}
		else {
			return new int[0];
		}
	}
	
	public TemporalInteger sliceByTime(long beginMillis, long endMillis) {
		TimedInteger[] pts = m_timeds.get();
		TimedInteger[] slice = FStream.of(pts)
									.dropWhile(tp -> tp.getTs() < beginMillis)
									.takeWhile(tp -> tp.getTs() < endMillis)
									.toArray(TimedInteger.class);
		return new TemporalInteger(slice);
	}
	
	public void add(TimedInteger tpt) {
		TimedInteger[] timeds = m_timeds.get();
		int idx = Arrays.binarySearch(timeds, tpt, COMTOR);
		idx = (idx >= 0) ? idx : -(idx + 1);
		
		TimedInteger[] added = new TimedInteger[timeds.length+1];
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
	
	public static final TemporalInteger merge(TemporalInteger tp1, TemporalInteger tp2) {
		int idx1 = 0;
		int idx2 = 0;
		
		TimedInteger[] timeds1 = tp1.getSamples();
		TimedInteger[] timeds2 = tp2.getSamples();
		List<TimedInteger> mergeds = Lists.newArrayList();
		while ( idx1 < timeds1.length && idx2 < timeds2.length ) {
			TimedInteger pt1 = timeds1[idx1];
			TimedInteger pt2 = timeds2[idx2];
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
		
		TimedInteger[] tps = mergeds.toArray(new TimedInteger[mergeds.size()]);
		return new TemporalInteger(tps);
	}
	
	public static TemporalInteger fromRow(Row row) {
		if ( row instanceof TemporalInteger ) {
			return (TemporalInteger)row;
		}
		else {
			return new TemporalInteger((WrappedArray)row.getAs(0));
		}
	}

	@Override
	public Row copy() {
		return m_timeds.isLoaded() ? new TemporalInteger(m_timeds.get()) : new TemporalInteger(m_wrapped);
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
		int npoints = getNumSamples();
		if ( npoints > 1) {
			TimedInteger first = getFirstSample();
			TimedInteger last = getLastSample();
			
			long millis = last.getTs() - first.getTs();
			long seconds = Duration.ofMillis(millis).getSeconds();
			String durStr = String.format("%02d:%02d:%02d", seconds/3600, (seconds%3600)/60, seconds%60);
			
			return String.format("%s[%d][%s:%s]", durStr, npoints, first, last);
		}
		else if ( npoints == 1 ) {
			TimedInteger first = getFirstSample();
			return String.format("[%d][%s]", npoints, first);
		}
		else {
			return "empty";
		}
	}
	
	private static final class TimeComparator implements Comparator<TimedInteger> {
		@Override
		public int compare(TimedInteger o1, TimedInteger o2) {
			return Long.compare(o1.getTs(), o2.getTs());
		}
	}
	
	public static TimedInteger[] toTimedIntegerArray(WrappedArray warray) {
		TimedInteger[] timeds = new TimedInteger[warray.length()];
		for ( int i =0; i < timeds.length; ++i ) {
			Row timedRow = (Row)warray.apply(i);
			timeds[i] = TimedInteger.fromRow(timedRow);
		}
		
		return timeds;
	}
}
