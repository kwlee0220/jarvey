package jarvey.type.temporal;

import java.util.List;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import jarvey.type.JarveyDataTypes;
import jarvey.type.temporal.BuildTemporalPointUDAF.MultiTemporalPointsBean;

import utils.UnitUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class BuildTemporalPointUDAF extends Aggregator<Row,MultiTemporalPointsBean,Row> {
	private static final long serialVersionUID = 1L;
	
	private static final Logger s_logger = LoggerFactory.getLogger(BuildTemporalPointUDAF.class);

	public static final StructType INPUT_SCHEMA = new StructType(new StructField[] {
		DataTypes.createStructField("tpoint", JarveyDataTypes.Temporal_Point_Type.getSparkType(), false),
	});
	public static final Encoder<Row> INPUT_ENCODER = RowEncoder.apply(INPUT_SCHEMA);
	
	@Override
	public MultiTemporalPointsBean zero() {
		return new MultiTemporalPointsBean();
	}

	@Override
	public MultiTemporalPointsBean reduce(MultiTemporalPointsBean buffer, Row input) {
		if ( input != null ) {
			Row tpRow = input.getAs(0);
			buffer.add(tpRow.getAs(3));
		}
		
		return buffer;
	}

	@Override
	public MultiTemporalPointsBean merge(MultiTemporalPointsBean buffer1, MultiTemporalPointsBean buffer2) {
		buffer1.addAll(buffer2);
		return buffer1;
	}

	@Override
	public Row finish(MultiTemporalPointsBean reduction) {
		return reduction.toRow();
	}

	@Override
	public Encoder<MultiTemporalPointsBean> bufferEncoder() {
		return MultiTemporalPointsBean.ENCODER;
	}

	@Override
	public Encoder<Row> outputEncoder() {
		return TemporalPointType.ENCODER;
	}

	public static class MultiTemporalPointsBean {
		public static final Encoder<MultiTemporalPointsBean> ENCODER = Encoders.bean(MultiTemporalPointsBean.class);
		
		private List<byte[]> m_tpBytesList = Lists.newArrayList();
		
		public byte[] getBytes() {
			if ( m_tpBytesList.size() > 1 ) {
				m_tpBytesList.clear();
				m_tpBytesList.add(mergeToRow().getAs(3));
			}
			return m_tpBytesList.get(0);
		}
		
		public void setBytes(byte[] bytes) {
			m_tpBytesList.add(bytes);
		}
		
		public void add(byte[] compressedBytes) {
			m_tpBytesList.add(compressedBytes);
		}
		
		public void addAll(MultiTemporalPointsBean bean) {
			m_tpBytesList.addAll(bean.m_tpBytesList);
		}
		
		public Row toRow() {
			if ( m_tpBytesList.size() > 1 ) {
				return mergeToRow();
			}
			else {
				return TemporalPoint.fromCompressedBytes(m_tpBytesList.get(0)).toRow();
			}
		}
		
		@Override
		public String toString() {
			return String.format("n_tps=%d", m_tpBytesList.size());
		}
		
		Row mergeToRow() {
			List<TemporalPoint> tpList = FStream.from(m_tpBytesList)
												.map(TemporalPoint::fromCompressedBytes)
												.toList();
			TemporalPoint merged = TemporalPoint.merge(tpList);
			Row row = merged.toRow();
			if ( s_logger.isInfoEnabled() ) {
				String splitsStr = FStream.from(tpList)
										.zipWith(FStream.from(m_tpBytesList))
										.map(tup -> toShortString(tup._1, tup._2))
										.join(", ");
				byte[] compressed = row.getAs(3);
				s_logger.info("merging {} tpoints: {}[{}, {}] <- {}",
								tpList.size(), merged.getDuration().toString().substring(2),
								merged.length(), UnitUtils.toByteSizeString(compressed.length), splitsStr);
			}
			
			return row;
		}
		
		private String toShortString(TemporalPoint tp, byte[] compressed) {
			return String.format("%s[%d, %s]", tp.getDuration().toString().substring(2), tp.length(),
												UnitUtils.toByteSizeString(compressed.length));
		}
	}	
}
