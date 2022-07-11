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
import org.locationtech.jts.geom.Point;

import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;
import utils.LocalDateTimes;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TimedPoint implements Row {
	private static final long serialVersionUID = 1L;

	private GeometryValue m_pt;
	private long m_ts;
	
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("point", GeometryType.DATA_TYPE, false),
		DataTypes.createStructField("ts", DataTypes.LongType, false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public static Row empty() {
		return new GenericRow();
	}
	
	public TimedPoint(GeometryValue ptRow, long ts) {
		m_pt = ptRow;
		m_ts = ts;
	}
	
	public GeometryValue getGeometryRow() {
		return m_pt;
	}
	
	public Point getPoint() {
		return (Point)m_pt.getGeometry();
	}
	
	public long getTs() {
		return m_ts;
	}
	
	public LocalDateTime getLocalDateTime() {
		return LocalDateTimes.fromEpochMillis(m_ts);
	}
	
	public static TimedPoint fromRow(Row row) {
		if ( row instanceof TimedPoint ) {
			return (TimedPoint)row;
		}
		else {
			GeometryValue ptRow = GeometryValue.from(row.getAs(0));
			long ts = row.getAs(1);
			return new TimedPoint(ptRow, ts);
		}
	}
	
	public static TimedPoint[] fromRowArray(Row[] rows) {
		return FStream.of(rows)
						.map(TimedPoint::fromRow)
						.toArray(TimedPoint.class);
	}

	@Override
	public Row copy() {
		return new TimedPoint(m_pt, m_ts);
	}

	@Override
	public Object get(int i) {
		if ( i == 0 ) {
			return m_pt;
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
		return new GenericInternalRow(new Object[]{m_pt, m_ts});
	}
	
	public String toString() {
		Point pt = (Point)m_pt.getGeometry();
		return String.format("(%.3f,%.3f)@%s", pt.getX(), pt.getY(), m_ts);
	}
}
