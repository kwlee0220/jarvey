package jarvey.type.temporal;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Comparator;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

import com.google.common.base.Objects;

import jarvey.support.GeoUtils;

import utils.LocalDateTimes;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TimedPoint implements Comparable<TimedPoint> {
	private final float m_x;
	private final float m_y;
	private final long m_t;
	
	public static final StructType DATA_TYPE = new StructType(new StructField[] {
		DataTypes.createStructField("x", DataTypes.FloatType, false),
		DataTypes.createStructField("y", DataTypes.FloatType, false),
		DataTypes.createStructField("t", DataTypes.TimestampType, false),
	});
	public static final Encoder<Row> ENCODER = RowEncoder.apply(DATA_TYPE);
	
	public TimedPoint(float x, float y, long t) {
		m_x = x;
		m_y = y;
		m_t = t;
	}
	
	public float getX() {
		return m_x;
	}
	
	public float getY() {
		return m_y;
	}
	
	public long getT() {
		return m_t;
	}
	
	public LocalDateTime toLocalDateTime() {
		return LocalDateTimes.fromEpochMillis(m_t);
	}
	
	public Timestamp getTimestamp() {
		return new Timestamp(m_t);
	}
	
	public Coordinate toCoordinate() {
		return new Coordinate(m_x, m_y);
	}
	
	public Point toPoint() {
		return GeoUtils.toPoint(toCoordinate());
	}
	
	public static final TimedPoint from(Row row) {
		return new TimedPoint(row.getFloat(0), row.getFloat(1), row.getTimestamp(2).getTime());
	}
	
	public Row toRow() {
		return RowFactory.create(m_x, m_y, new Timestamp(m_t));
	}

	@Override
	public String toString() {
		return String.format("(%.3f,%.3f)@%s", m_x, m_y, getTimestamp());
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != TimedPoint.class ) {
			return false;
		}
		
		TimedPoint other = (TimedPoint)obj;
		return Objects.equal(m_x, other.m_x)
			&& Objects.equal(m_y, other.m_y)
			&& Objects.equal(m_t, other.m_t);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(m_x, m_y, m_t);
	}

	@Override
	public int compareTo(TimedPoint o) {
		return Long.compare(m_t, o.m_t);
	}
	
	public static Comparator<TimedPoint> COMPTOR = new Comparator<TimedPoint>() {
		@Override
		public int compare(TimedPoint o1, TimedPoint o2) {
			return Long.compare(o1.getT(), o2.getT());
		}
	};
}
