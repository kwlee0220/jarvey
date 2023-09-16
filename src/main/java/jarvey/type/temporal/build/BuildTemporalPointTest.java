package jarvey.type.temporal.build;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveySession;
import jarvey.datasource.DatasetException;
import jarvey.support.Rows;
import jarvey.support.SchemaUtils;
import jarvey.type.JarveyDataTypes;

import utils.UnitUtils;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTemporalPointTest {
	static final Logger s_logger = LoggerFactory.getLogger(BuildTemporalPointTest.class);
	
	private static final String TPOINT_COL_NAME = "tpoint";
	public static final String PERIOD_COL_NAME = "period_id";
	public static final String X = "_x";
	public static final String Y = "_y";
	public static final String T = "_t";

	private final JarveySession m_jarvey;
	private final String m_outputDsId;
	private final String[] m_keyColNames;
	private final String[] m_xytColNames;
	private long m_periodMillis = UnitUtils.parseDurationMillis("30m");
	private int m_mergingPartCount = 2000;
	
	public BuildTemporalPointTest(JarveySession jarvey, String[] keyColNames, String[] xytColNames,
								String outputDsId) {
		m_jarvey = jarvey;
		m_outputDsId = outputDsId;
		m_keyColNames = keyColNames;
		m_xytColNames = xytColNames;
	}
	
	public void setPeriod(long periodMillis) {
		m_periodMillis = periodMillis;
	}
	public void setPeriod(String period) {
		m_periodMillis = UnitUtils.parseDurationMillis(period);
	}
	
	public void setMergingPartitionCount(int count) {
		m_mergingPartCount = count;
	}
	
	public void run(Dataset<Row> input) {
		Dataset<Row> rows;
		
		rows = normalizeInput(input);

		Column[] extKeyCols = Rows.toColumns(rows, m_keyColNames, PERIOD_COL_NAME);
		Column[] xytCols = Rows.toColumns(rows, X, Y, T);
		rows = rows.groupBy(extKeyCols).agg(callUDF("BuildTPoint", xytCols));
		rows.printSchema();
		rows.show(5);
		
//		m_jarvey.toSpatial(rows)
//				.writeSpatial()
//				.partitionBy(BuildTemporalPoint.PERIOD_COL_NAME)
//				.force(true)
//				.dataset(m_outputDsId);
	}
	
	private Dataset<Row> normalizeInput(Dataset<Row> input) {
		// key값과 좌표값이 null인 레코드는 제외시킨다.
		Column keyNotNull = FStream.of(Rows.toColumns(input, m_keyColNames, m_xytColNames))
									.map(c -> c.isNotNull())
									.reduce((c1, c2) -> c1.and(c2));
		input = input.filter(keyNotNull);
		
		// TemporalPoint 데이터 생성에 필요한 데이터만 뽑는다.
		Column[] keyCols = Rows.toColumns(input, m_keyColNames);
		Column[] xytCols = new Column[]{ input.col(m_xytColNames[0]).as(X),
										input.col(m_xytColNames[1]).as(Y),
										input.col(m_xytColNames[2]).as(T) };
		input = input.select(Utilities.concat(keyCols, xytCols));
		
		// x, y 좌표 값을 float 타입을 casting 시킨다.
		input = input.withColumn(X, input.col(X).cast("float"))
					.withColumn(Y, input.col(Y).cast("float"));
		
		// sample timestamp 컬럼의 type이 'Timestamp'인 경우에는
		// long (milli-second)로 변환시킨다. 
		int tColIdx = input.schema().fieldIndex(T);
		DataType tType = input.schema().fields()[tColIdx].dataType();
		if ( tType.equals(DataTypes.TimestampType) ) {
			input = input.withColumn(T, expr(String.format("unix_millis(%s)", T)));
		}
		else if ( !input.schema().fields()[tColIdx].dataType().equals(DataTypes.LongType) ) {
			throw new DatasetException("invalid timestamp type: column=" + m_xytColNames[2]);
		}

		// TemporalPoint 데이터가 너무 커져서 Out-Of-Memory 예외가 발생하는 것을 방지하기 위해
		// 데이터를 일정기간 단위(m_segmentPeriodMillis)로 분할하기 위한 salt 컬럼을 생성한다.
		String periodExpr = String.format("cast(%s / %d as int) as %s", T, m_periodMillis, PERIOD_COL_NAME);
		input = input.selectExpr(Utilities.concat(m_keyColNames, periodExpr, X, Y, T));
		
		return input;
	}
	
	private Dataset<Row> buildTemporalPointWithinPartition(Dataset<Row> input) {
		// input schema: key1, key2, ..., keyn, x, y, t

		// TemporalPoint 데이터가 너무 커져서 Out-Of-Memory 예외가 발생하는 것을 방지하기 위해
		// 데이터를 일정기간 단위(m_segmentPeriodMillis)로 분할하기 위한 salt 컬럼을 생성한다.
		String periodExpr = String.format("cast(%s / %d as int) as %s", T, m_periodMillis, PERIOD_COL_NAME);
		input = input.selectExpr(Utilities.concat(m_keyColNames, periodExpr, X, Y, T));
		
		Column[] keyCols = Rows.toColumns(input, m_keyColNames);
		StructType outSchema = SchemaUtils.toBuilder(input.select(keyCols).schema())
										.addOrReplaceField(PERIOD_COL_NAME, DataTypes.IntegerType)
										.addOrReplaceField("tpoint", JarveyDataTypes.Temporal_Point_Type.getSparkType())
										.build();
		input = input.mapPartitions(new BuildInPartitionTPoint(keyCols.length+1),
									RowEncoder.apply(outSchema));
		return input;
	}
	
	private Dataset<Row> mergeTemporalPoints(Dataset<Row> rows) {
		// input schema: key1, key2, ..., keyn, period_id, tpoint

		// ("trans_reg_num", "driver_code", "period_id")를 기준으로 groupping하여
		// TemporalPoint를 생성한다.
		Dataset<Row> output;
		Column[] extKeyCols = Rows.toColumns(rows, m_keyColNames, PERIOD_COL_NAME);
		output = rows.repartition(m_mergingPartCount, extKeyCols)
					.groupBy(extKeyCols).agg(collect_list(TPOINT_COL_NAME).as(TPOINT_COL_NAME));
		
		int tpColIdx = output.schema().fieldIndex(TPOINT_COL_NAME);
		output = output.mapPartitions(new MergeTPoints(tpColIdx), rows.encoder());
		
		return output;
	}
}
