package jarvey.type.temporal.build;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;
import jarvey.optor.MapIterator;
import jarvey.support.MapTile;
import jarvey.support.SchemaUtils;
import jarvey.type.temporal.TemporalPoint;

import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateTemporalPointIndex {
	static final Logger s_logger = LoggerFactory.getLogger(CreateTemporalPointIndex.class);

	private transient JarveySession m_jarvey;
	private final long[] m_qids;
	private final String m_outputDsId;
	private final int m_outputPartCount;
	
	public CreateTemporalPointIndex(JarveySession jarvey, long qids[], String outputDsId, int outputPartCount) {
		m_jarvey = jarvey;
		m_qids = qids;
		m_outputDsId = outputDsId;
		m_outputPartCount = outputPartCount;
	}
	
	public void run(Dataset<Row> input) {
		CreateIndex create = new CreateIndex(input.schema(), m_qids);
		Dataset<Row> idx = input.mapPartitions(create, create.getOutputEncoder())
								.repartition(m_outputPartCount);
		
		m_jarvey.toSpatial(idx)
				.writeSpatial()
				.force(true)
				.dataset(m_outputDsId);
	}

	static class CreateIndex implements MapPartitionsFunction<Row, Row> {
		private static final long serialVersionUID = 1L;
		private static final Logger s_logger = CreateTemporalPointIndex.s_logger;
		
		private final int[] m_extKeyColIdxes;
		private final int m_tpColIdx;
		private final long[] m_qids;
		private final StructType m_outSchema;
		private transient List<MapTile> m_tiles;
		
		CreateIndex(StructType inputSchema, long[] qids) {
			m_tpColIdx = inputSchema.fieldIndex("tpoint");
			
			String[] keyColNames = SchemaUtils.complement(inputSchema, Arrays.asList("tpoint", "period_id"))
												.fieldNames();
			String[] extKeyColNames = Utilities.concat(new String[]{"period_id"}, keyColNames);
			m_extKeyColIdxes = SchemaUtils.fieldIndexes(inputSchema, extKeyColNames);
			
			m_qids = qids;
			m_outSchema = SchemaUtils.toBuilder(SchemaUtils.select(inputSchema, extKeyColNames))
									.addOrReplaceField("length", DataTypes.IntegerType)
									.addOrReplaceField("first_ts", DataTypes.LongType)
									.addOrReplaceField("last_ts", DataTypes.LongType)
									.addOrReplaceField("size", DataTypes.IntegerType)
									.addOrReplaceField("quad_ids", DataTypes.createArrayType(DataTypes.LongType))
									.build();
		}
		
		public StructType getOutputSchema() {
			return m_outSchema;
		}
		
		public Encoder<Row> getOutputEncoder() {
			return RowEncoder.apply(m_outSchema);
		}
	
		@Override
		public Iterator<Row> call(Iterator<Row> iter) throws Exception {
			m_tiles = FStream.of(m_qids).map(MapTile::fromQuadId).toList();
			
			MapIterator<Row,Row> outIter = new MapIterator<Row,Row>(iter) {
				@Override
				protected Row apply(Row row) {
					return createIndexEntry(row);
				}
				
				@Override
				protected String getHandlerString() {
					return CreateIndex.class.getSimpleName();
				}
			};
			outIter.setLogger(s_logger);
			outIter.start();
			
			return outIter;
		}
		
		private Row createIndexEntry(Row row) {
			Row tpRow = row.getAs(m_tpColIdx);
			TemporalPoint tp = TemporalPoint.from(tpRow);

			int keyLen = m_extKeyColIdxes.length;
			Object[] entry = new Object[m_outSchema.length()];
			for ( int i =0; i < keyLen; ++i ) {
				entry[i] = row.get(m_extKeyColIdxes[i]);
			}
			entry[keyLen] = tp.length();
			entry[keyLen+1] = tp.getFirstSample().getT();
			entry[keyLen+2] = tp.getLastSample().getT();
			entry[keyLen+3] = ((byte[])tpRow.get(3)).length;
			
			Long[] qids = FStream.from(tp.getContainingMapTiles(m_tiles))
									.map(MapTile::getQuadId)
									.toArray(Long.class);
			entry[keyLen+4] = qids;
			
			return RowFactory.create(entry);
		}
	}
}
