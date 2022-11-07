package jarvey.type.temporal.build;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.optor.MapIterator;
import jarvey.support.Rows;
import jarvey.type.temporal.TemporalPoint;

import scala.collection.mutable.WrappedArray;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MergeTPoints implements MapPartitionsFunction<Row, Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(MergeTPoints.class);
	
	private final int m_tpColIdx;
	
	MergeTPoints(int tpColIdx) {
		m_tpColIdx = tpColIdx;
	}

	@Override
	public Iterator<Row> call(Iterator<Row> iter) throws Exception {
		MapIterator<Row,Row> outIter = new MapIterator<Row,Row>(iter) {
			@Override
			protected Row apply(Row row) {
				return mergeTPoints(row);
			}
			
			@Override
			protected String getHandlerString() {
				return MergeTPoints.class.getSimpleName();
			}
		};
		outIter.setLogger(s_logger);
		outIter.start();
		
		return outIter;
	}
	
	private Row mergeTPoints(Row row) {
		Object[] values = Rows.toArray(row);
		
		WrappedArray<Row> tpRows = row.getAs(m_tpColIdx);
		int length = tpRows.length();
		if ( length == 1 ) {
			values[m_tpColIdx] = tpRows.apply(0);
		}
		else {
			List<Row> rowSplits = FStream.range(0, length).map(tpRows::apply).toList();
			List<TemporalPoint> splits = FStream.from(rowSplits).map(TemporalPoint::from).toList();
			TemporalPoint merged = TemporalPoint.merge(splits);
			Row mergedRow = merged.toRow();
			values[m_tpColIdx] = mergedRow;
			
			if ( s_logger.isDebugEnabled() ) {
				String key = Arrays.toString(Arrays.copyOf(values, m_tpColIdx));
				String splitsStr = FStream.from(splits)
										.zipWith(FStream.from(rowSplits))
										.map(tup -> toShortString(tup._1, tup._2.getAs(3)))
										.join(", ");
				byte[] compressed = mergedRow.getAs(3);
				s_logger.debug("key={}, merging {} tpoints: {}[{}, {}] <- {}",
								key, splits.size(), merged.getDuration().toString().substring(2),
								merged.length(), UnitUtils.toByteSizeString(compressed.length), splitsStr);
			}
		}
		
		return RowFactory.create(values);
	}
	
	private String toShortString(TemporalPoint tp, byte[] compressed) {
		return String.format("%s[%d, %s]", tp.getDuration().toString().substring(2), tp.length(),
											UnitUtils.toByteSizeString(compressed.length));
	}
}