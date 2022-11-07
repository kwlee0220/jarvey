package jarvey.type.temporal;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Objects;

import utils.LocalDateTimes;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class Timed<T> implements Comparable<Timed<T>> {
	private final T m_data;
	private final long m_t;
	
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("x", DataTypes.DoubleType, false),
		DataTypes.createStructField("y", DataTypes.DoubleType, false),
		DataTypes.createStructField("t", DataTypes.LongType, false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public Timed(T data, long t) {
		m_data = data;
		m_t = t;
	}
	
	public T getData() {
		return m_data;
	}
	
	public long getT() {
		return m_t;
	}
	
	public Timestamp toTimestamp() {
		return new Timestamp(m_t);
	}
	
	public LocalDateTime toLocalDateTime() {
		return LocalDateTimes.fromEpochMillis(m_t);
	}

	@Override
	public String toString() {
		return String.format("%s@%s", m_data, toTimestamp());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != Timed.class ) {
			return false;
		}
		
		Timed<T> other = (Timed)obj;
		return Objects.equal(m_data, other.m_data) && Objects.equal(m_t, other.m_t);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_data, m_t);
	}

	@Override
	public int compareTo(Timed<T> o) {
		return Long.compare(m_t, o.m_t);
	}
}
