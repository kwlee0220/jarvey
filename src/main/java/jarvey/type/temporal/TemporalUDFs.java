package jarvey.type.temporal;

import java.io.Serializable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import jarvey.support.GeoUtils;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TemporalUDFs {
	private TemporalUDFs() {
		throw new AssertionError("Should not be called: class=" + TemporalUDFs.class);
	}
	
	static interface TemporalPointFunction<T> extends Serializable {
		public T apply(TemporalPoint tpt);
	}
	private static final <T> UDF1<Row, T> adapt(TemporalPointFunction<T> func) {
		return (tpRow) -> func.apply(TemporalPoint.from(tpRow));
	};
	
	static interface TemporalPointArg1Function<IN,T> extends Serializable {
		public T apply(TemporalPoint tpt, IN arg1);
	}
	private static final <IN,T> UDF2<Row,IN,T> adapt(TemporalPointArg1Function<IN,T> func) {
		return (tpRow, arg1) -> func.apply(TemporalPoint.from(tpRow), arg1);
	};
	
	public static void registerUdf(UDFRegistration registry) {
		registry.register("TP_SampleAtIndex", TP_SampleAtIndex, TimedPoint.DATA_TYPE);
		registry.register("TP_FirstSample", TP_FirstSample, TimedPoint.DATA_TYPE);
		registry.register("TP_LastSample", TP_LastSample, TimedPoint.DATA_TYPE);
		
		registry.register("TP_LineString", adapt(TP_LineString), DataTypes.BinaryType);
	}

	private static final FOption<TemporalPoint> toTemporalPoint(Row tpRow) {
		return (tpRow != null) ? FOption.of(TemporalPoint.from(tpRow)) : FOption.empty();
	}
	private static final FOption<TimedPoint> toTimedPoint(Row tpRow, int idx) {
		return (tpRow != null) ? FOption.of(TemporalPoint.from(tpRow)).map(tp -> tp.getSample(idx)) : FOption.empty();
	}
	private static final UDF2<Row,Integer,Row> TP_SampleAtIndex = (tpRow, idx) -> {
		return toTimedPoint(tpRow, idx).map(TimedPoint::toRow).getOrNull();
	};
	private static final UDF1<Row,Row> TP_FirstSample = (tpRow) -> {
		return toTimedPoint(tpRow, 0).map(TimedPoint::toRow).getOrNull();
	};
	private static final UDF1<Row,Row> TP_LastSample = (tpRow) -> {
		return toTimedPoint(tpRow, -1).map(TimedPoint::toRow).getOrNull();
	};

	private static final TemporalPointFunction<byte[]> TP_LineString = (tp) -> {
		return GeoUtils.toWKB(tp.toLineString());
	};
	
	private static final TimedPoint getSample(Row tpRow, int index) {
		if ( tpRow != null ) {
			return TemporalPoint.from(tpRow).getSample(index);
		}
		else {
			return null;
		}
	}
}
