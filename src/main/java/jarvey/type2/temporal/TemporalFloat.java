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
public class TemporalFloat implements Row {
	private static final long serialVersionUID = 1L;

	private Lazy<TimedFloat[]> m_timeds;
	private WrappedArray<Row> m_wrapped;
	
	private static final TimeComparator COMTOR = new TimeComparator();
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("samples", DataTypes.createArrayType(TimedFloat.DATA_TYPE), false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public TemporalFloat() {
		this(new TimedFloat[0]);
	}
	
	public TemporalFloat(TimedFloat[] timeds) {
		m_timeds = Lazy.of(timeds);
		m_wrapped = null;
	}
	
	public TemporalFloat(WrappedArray<Row> timedsArray) {
		m_timeds = Lazy.of(() -> TemporalFloat.toTimedFloatArray(timedsArray));
		m_wrapped = timedsArray;
	}
	
	public int getNumSamples() {
		return m_timeds.isLoaded() ? m_timeds.get().length : m_wrapped.length();
	}
	
	public TimedFloat[] getSamples() {
		return m_timeds.get();
	}
	
	public TimedFloat getSampleAt(int idx) {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[idx];
		}
		else {
			return TimedFloat.fromRow(m_wrapped.apply(idx));
		}
	}
	
	public TimedFloat getFirstSample() {
		if ( m_timeds.isLoaded() ) {
			return m_timeds.get()[0];
		}
		else {
			return TimedFloat.fromRow(m_wrapped.head());
		}
	}
	
	public TimedFloat getLastSample() {
		if ( m_timeds.isLoaded() ) {
			int npoints = getNumSamples();
			return m_timeds.get()[npoints-1];
		}
		else {
			return TimedFloat.fromRow(m_wrapped.last());
		}
	}
	
	public float getFirstValue() {
		return getFirstSample().getValue();
	}
	
	public float getLastValue() {
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
	
	public float[] getValues() {
		TimedFloat[] timeds = m_timeds.get();
		
		if ( timeds.length > 1 ) {
			return FStream.of(m_timeds.get())
							.mapToFloat(TimedFloat::getValue)
							.toArray();
		}
		else if ( timeds.length == 1 ) {
			return new float[]{timeds[0].getValue()};
		}
		else {
			return new float[0];
		}
	}
	
	public TemporalFloat sliceByTime(long beginMillis, long endMillis) {
		TimedFloat[] pts = m_timeds.get();
		TimedFloat[] slice = FStream.of(pts)
									.dropWhile(tp -> tp.getTs() < beginMillis)
									.takeWhile(tp -> tp.getTs() < endMillis)
									.toArray(TimedFloat.class);
		return new TemporalFloat(slice);
	}
	
	public void add(TimedFloat tpt) {
		TimedFloat[] timeds = m_timeds.get();
		int idx = Arrays.binarySearch(timeds, tpt, COMTOR);
		idx = (idx >= 0) ? idx : -(idx + 1);
		
		TimedFloat[] added = new TimedFloat[timeds.length+1];
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
	
	public static final TemporalFloat merge(TemporalFloat tp1, TemporalFloat tp2) {
		int idx1 = 0;
		int idx2 = 0;
		
		TimedFloat[] timeds1 = tp1.getSamples();
		TimedFloat[] timeds2 = tp2.getSamples();
		List<TimedFloat> mergeds = Lists.newArrayList();
		while ( idx1 < timeds1.length && idx2 < timeds2.length ) {
			TimedFloat pt1 = timeds1[idx1];
			TimedFloat pt2 = timeds2[idx2];
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
		
		TimedFloat[] tps = mergeds.toArray(new TimedFloat[mergeds.size()]);
		return new TemporalFloat(tps);
	}
	
	public static TemporalFloat fromRow(Row row) {
		if ( row instanceof TemporalFloat ) {
			return (TemporalFloat)row;
		}
		else {
			return new TemporalFloat((WrappedArray)row.getAs(0));
		}
	}

	@Override
	public Row copy() {
		return m_timeds.isLoaded() ? new TemporalFloat(m_timeds.get()) : new TemporalFloat(m_wrapped);
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
			TimedFloat first = getFirstSample();
			TimedFloat last = getLastSample();
			
			long millis = last.getTs() - first.getTs();
			long seconds = Duration.ofMillis(millis).getSeconds();
			String durStr = String.format("%02d:%02d:%02d", seconds/3600, (seconds%3600)/60, seconds%60);
			
			return String.format("%s[%d][%s:%s]", durStr, npoints, first, last);
		}
		else if ( npoints == 1 ) {
			TimedFloat first = getFirstSample();
			return String.format("[%d][%s]", npoints, first);
		}
		else {
			return "empty";
		}
	}
	
	private static final class TimeComparator implements Comparator<TimedFloat> {
		@Override
		public int compare(TimedFloat o1, TimedFloat o2) {
			return Long.compare(o1.getTs(), o2.getTs());
		}
	}
	
	public static TimedFloat[] toTimedFloatArray(WrappedArray warray) {
		TimedFloat[] timeds = new TimedFloat[warray.length()];
		for ( int i =0; i < timeds.length; ++i ) {
			Row timedRow = (Row)warray.apply(i);
			timeds[i] = TimedFloat.fromRow(timedRow);
		}
		
		return timeds;
	}
}
