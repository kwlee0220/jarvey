/**
 * 
 */
package jarvey.type2.temporal;

import java.time.LocalDateTime;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import utils.LocalDateTimes;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TimedInteger implements Row {
	private static final long serialVersionUID = 1L;

	private int m_value;
	private long m_ts;
	
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("value", DataTypes.IntegerType, false),
		DataTypes.createStructField("ts", DataTypes.LongType, false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public static Row empty() {
		return new GenericRow();
	}
	
	public TimedInteger(int value, long ts) {
		m_value = value;
		m_ts = ts;
	}
	
	public int getValue() {
		return m_value;
	}
	
	public long getTs() {
		return m_ts;
	}
	
	public LocalDateTime getLocalDateTime() {
		return LocalDateTimes.fromEpochMillis(m_ts);
	}
	
	public static TimedInteger fromRow(Row row) {
		if ( row instanceof TimedInteger ) {
			return (TimedInteger)row;
		}
		else {
			int value = row.getAs(0);
			long ts = row.getAs(1);
			return new TimedInteger(value, ts);
		}
	}
	
	public static TimedInteger[] fromRowArray(Row[] rows) {
		return FStream.of(rows)
						.map(TimedInteger::fromRow)
						.toArray(TimedInteger.class);
	}

	@Override
	public Row copy() {
		return new TimedInteger(m_value, m_ts);
	}

	@Override
	public Object get(int i) {
		if ( i == 0 ) {
			return m_value;
		}
		else if ( i == 1 ) {
			return m_ts;
		}
		else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public int length() {
		return 2;
	}
	
	public InternalRow toInternalRow() {
		return new GenericInternalRow(new Object[]{m_value, m_ts});
	}
	
	public String toString() {
		return String.format("%d@%s", m_value, m_ts);
	}
}
