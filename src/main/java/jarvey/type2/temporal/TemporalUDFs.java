package jarvey.type2.temporal;

import java.io.Serializable;
import java.sql.Timestamp;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.type.GeometryType;
import jarvey.type.GeometryValue;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalUDFs {
	private static final Logger s_logger = LoggerFactory.getLogger(TemporalUDFs.class);
	
	private TemporalUDFs() {
		throw new AssertionError("Should not be called: class=" + TemporalUDFs.class);
	}
	
	static interface TemporalPointFunction<T> extends Serializable {
		public T call(TemporalPoint temporal);
	}
	private static final <T> UDF1<Row, T> adapt(TemporalPointFunction<T> func) {
		return row -> func.call(TemporalPoint.fromRow(row));
	};
	
	public static void registerUdf(UDFRegistration registry) {
		registry.register("TP_NumPoints", adapt(TP_NumPoints), DataTypes.IntegerType);
		registry.register("TP_Path", adapt(TP_Path), GeometryType.DATA_TYPE);
		registry.register("TP_Duration", adapt(TP_Duration), DataTypes.LongType);
		registry.register("TP_FirstTimestamp", adapt(TP_FirstTimestamp), DataTypes.TimestampType);
		registry.register("TP_LastTimestamp", adapt(TP_LastTimestamp), DataTypes.TimestampType);
	}
	
	private static final TemporalPointFunction<Integer> TP_NumPoints = TemporalPoint::getNumPoints;
	private static final TemporalPointFunction<GeometryValue> TP_Path = temporal -> new GeometryValue(temporal.getPath());
	private static final TemporalPointFunction<Long> TP_Duration = TemporalPoint::getDuration;
	private static final TemporalPointFunction<Timestamp> TP_FirstTimestamp = TemporalPoint::getFirstTimestamp;
	private static final TemporalPointFunction<Timestamp> TP_LastTimestamp = TemporalPoint::getLastTimestamp;
}
