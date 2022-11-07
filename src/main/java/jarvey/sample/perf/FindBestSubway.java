package jarvey.sample.perf;

import static jarvey.optor.geom.join.SpatialJoinOptions.DIFFERENCE_JOIN;
import static org.apache.spark.sql.functions.aggregate;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.locationtech.jts.geom.Envelope;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;
import jarvey.optor.geom.SquareGrid;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.support.DataFrames;
import jarvey.type.GeometryColumnInfo;

import utils.Size2d;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestSubway implements Callable<Void> {
	private static final String SID = "구역/시도";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TAXI_LOG = "교통/나비콜";
	private static final String BLOCKS = "경제/지오비전/집계구/2015";
	private static final String FLOW_POP_BYTIME = "경제/지오비전/유동인구/2015/시간대";
	private static final String CARD_BYTIME = "경제/지오비전/카드매출/2015/시간대";
	private static final String RESULT = "분석결과/지하철역사_추천";
	
	private static final int SRID = 5179;
	
	private final JarveySession m_jarvey;
	private final String m_outDsId;
	
	public FindBestSubway(JarveySession jarvey, String outDsId) {
		m_jarvey = jarvey;
		m_outDsId = outDsId;
	}
	
	public static final void main(String... args) throws Exception {
		// Jarvey Session 초기화.
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("find_best_subway_station_spots")
											.master("local[7]")
											.getOrCreate();

		StopWatch watch = StopWatch.start();
		FindBestSubway app = new FindBestSubway(jarvey, RESULT);
		app.call();
		
		SpatialDataFrame sdf = jarvey.read().dataset(RESULT);
		System.out.printf("종료: %s(%d건), 소요시간=%ss%n",
							RESULT, sdf.count(), watch.getElapsedMillisString());
	}
	
	public Void call() throws Exception {
		// '전국_지하쳘_역사' 중 서울 소재 역사 추출 후 1KM 버퍼 계산
		SpatialDataFrame seoulStations = bufferSeoulSubwayStations(m_jarvey);
		
		// 서울 영역만의 grid를 구하기 위해 서울 영역의 MBR을 구하고
		// 300x300 미터 cell의 grid를 생성함
		SquareGrid grid = getSeoulGrid(m_jarvey);

		// 격자별 집계구 비율 계산
		SpatialDataFrame blockRatios = calcBlockRatio(m_jarvey, seoulStations, grid).cache();
		
		// 격자별 택시 승하차 수 집계
		SpatialDataFrame taxi = gridTaxiLog(m_jarvey, blockRatios);
		
		// 격자별 유동인구 집계
		SpatialDataFrame flowPop = gridFlowPopulation(m_jarvey, blockRatios);
		
		// 격자별 카드매출 집계
		SpatialDataFrame cards = gridCardSales(m_jarvey, blockRatios);
		
		// 격자별_유동인구_카드매출_택시승하차_비율 합계
		SpatialDataFrame mergeds = mergeAll(m_jarvey, taxi, flowPop, cards);
		
		// 공간객체 부여
		SpatialDataFrame result = attachGeom(m_jarvey, mergeds, blockRatios);
		
		result.writeSpatial().force(true).dataset(m_outDsId);
		
		return null;
	}

	static SpatialDataFrame bufferSeoulSubwayStations(JarveySession jarvey) {
		return jarvey.read().dataset(STATIONS)
					.filter("substring(sig_cd, 0, 2) = '11'")
					.select("the_geom")
					.transformCrs(SRID)
					.buffer(1000);
	}
	
	static SquareGrid getSeoulGrid(JarveySession jarvey) {
		Envelope bounds = jarvey.read().dataset(SID)
								.filter("ctprvn_cd = 11")
								.transformCrs(SRID)
								.summarizeSpatialInfo()
								.getBounds();
		return new SquareGrid(bounds, new Size2d(300, 300));
	}

	static SpatialDataFrame calcBlockRatio(JarveySession jarvey, SpatialDataFrame seoulStations,
											SquareGrid grid) {
		// 격자별 집계구 비율 계산
		return jarvey.read().dataset(BLOCKS)	// 지오비전_집계구 데이터를 읽는다
				// 서울 영역의 집계구만 선택한다.
				.filter("substring(block_cd,0,2) == '11'")
				
				// '서울 소재 지하철 역사 1KM 영역' 데이터와 공간 조인을 위한 좌표계를 맞춘다.
				.transformCrs(SRID)
				// 서울 영역 집계구에서 서울 소재 지하철 역사 1KM 영역을 제외시킴
				.spatialBroadcastJoin(seoulStations, DIFFERENCE_JOIN())
				
				// 역사 원거리 서울 영역에  300x300 미터 grid cell을 부여한다.
				.assignGridCell(grid, false)		// add: cell_geom, cell_id, cell_pos
				.intersection_with("cell_geom")		// Grid cell 영역 중 원거리 서울 영역 교차 부분 계산
				.withRegularColumn("portion", expr("ST_Area(the_geom) / ST_Area(cell_geom)"))
				
				.select("the_geom", "cell_id", "cell_pos", "block_cd", "portion");
	}

	static SpatialDataFrame gridTaxiLog(JarveySession jarvey, SpatialDataFrame blocks) {
		GeometryColumnInfo blockGcInfo = blocks.assertDefaultGeometryColumnInfo();
		
		SpatialDataFrame taxi = jarvey.read().dataset(TAXI_LOG);
		GeometryColumnInfo taxiGcInfo = taxi.assertDefaultGeometryColumnInfo();
		
		String outJoinCols = String.format("left.*-{%s},right.*-{%s,cell_pos}",
											taxiGcInfo.getName(), blockGcInfo.getName());
		SpatialJoinOptions opts = SpatialJoinOptions.DEFAULT.outputColumns(outJoinCols);
		
		Dataset<Row> rows = taxi
				// 택시 로그 중에서 승차/하차 관련 로그만 선택한다.
				.filter("status = 1 or status = 2")
				// 격자별 집계구 데이터와 공간 조인을 위한 좌표계를 맞춘다.
				.transformCrs(blockGcInfo.getSrid())
				// 공간 조인을 통해 각 로그 레코드에 격자 정보를 부여한다.
				.spatialBroadcastJoin(blocks, opts)
				// 추후 작업을 위한 DataFrame으로 변환시킨다.
				.toDataFrame()
				// 격자별로 택시 승하차 횟수를 누적시킨다.
				.groupBy("cell_id").agg(count("*").cast("float").alias("count"));

		// 집계구별  값을 0 ~ 1 사이 값으로 scaling 한다.
		rows = DataFrames.scaleMinMax(rows, "count", "scaled_visit_count", 0, 1)
						.drop("count");
		
		return jarvey.toSpatial(rows);
	}

	private static Column average(Column arrCol) {
		return aggregate(arrCol, lit(0.0), (a,v) -> a.plus(v)).divide(size(arrCol));
	}
	static SpatialDataFrame gridFlowPopulation(JarveySession jarvey, SpatialDataFrame blocks) {
		Dataset<Row> avgPops;
		
		// 유동인구를  읽는다.
		SpatialDataFrame sdf= jarvey.read().dataset(FLOW_POP_BYTIME);
		
		// 서울지역 데이터만 선택
		sdf = sdf.filter(sdf.col("block_cd").startsWith("11"));
		
		// 하루동안의 시간대별 평균 유동인구를 계산
		String[] avgCols = FStream.range(0, 24)
									.map(i -> String.format("AVG_%02dTMST", i))
									.toArray(String.class);
		sdf = sdf.toArray(avgCols, "block_pop").drop(avgCols);
		sdf = sdf.withRegularColumn("avg_block_pop", average(sdf.col("block_pop")))
					.drop("block_pop", "std_ym");
		
		// 집계구별 평균 일간 유동인구 평균 계산
		avgPops = sdf.groupBy(sdf.col("block_cd")).agg(avg("avg_block_pop").as("avg_pop"));
		
		// 격자 정보 (portion 데이터) 부가함
		Dataset<Row> blocksDf = blocks.toDataFrame().select("block_cd", "cell_id", "portion");
		avgPops = avgPops.join(blocksDf, "block_cd");
		
		// 비율 값 반영: avg_pop = avg_pop * portion
		avgPops = avgPops.withColumn("avg_pop", expr("avg_pop * portion"));
		
		// 격자별 평균 유동인구을 계산한다.
		avgPops = avgPops.repartition(1, avgPops.col("cell_id"))
						.groupBy("cell_id").agg(sum("avg_pop").as("avg_pop"));

		// 평균 유동인구 값을 0 ~ 1 사이 값으로 scaling 한다.
		avgPops = DataFrames.scaleMinMax(avgPops, "avg_pop", "scaled_avg_pop", 0, 1)
							.drop("avg_pop");
		
		return jarvey.toSpatial(avgPops);
	}

	@SuppressWarnings("unused")
	static SpatialDataFrame gridCardSales(JarveySession jarvey, SpatialDataFrame blocks) {
		// 카드매출을  읽는다.
		SpatialDataFrame sdf = jarvey.read().dataset(CARD_BYTIME);
		
		// 서울지역 데이터만 선택
		sdf = sdf.filter(sdf.col("block_cd").startsWith("11"));
		
		// 하루동안의 카드매출 합계를 계산
		String[] saleCols = FStream.range(0, 24).map(i -> String.format("SALE_AMT_%02dTMST", i)).toArray(String.class);
		sdf = sdf.toArray(saleCols, "sales").drop(saleCols);
		sdf = sdf.withRegularColumn("sales", average(sdf.col("sales")));
		
		// 하루동안의 APV_CNT 합계를 계산
		String[] apvCols = FStream.range(0, 24).map(i -> String.format("APV_CNT_%02dTMST", i)).toArray(String.class);
		sdf = sdf.toArray(apvCols, "apvCnt").drop(apvCols);
		sdf = sdf.withRegularColumn("apvCnt", average(sdf.col("apvCnt")));
		
		// 하루동안의 가격 합계를 계산
		String[] priceCols = FStream.range(0, 24).map(i -> String.format("PRICE_%02dTMST", i)).toArray(String.class);
		sdf = sdf.toArray(priceCols, "price").drop(priceCols);
		sdf = sdf.withRegularColumn("price", average(sdf.col("price")));
		
		// 집계구별 매출액 총합 계산
		Dataset<Row> rows = sdf.repartition(3, sdf.col("block_cd"))
								.groupBy(sdf.col("block_cd")).agg(sum("sales").as("sum_sales"));
		
		// 격자 정보 (portion 데이터) 부가함
		rows = rows.join(blocks.toDataFrame(), "block_cd");
		
		// 비율 값 반영
		rows = rows.withColumn("sum_sales", expr("sum_sales * portion"));
		
		// 격자별 평균 카드매출액을 계산한다.
		rows = rows.repartition(1, rows.col("cell_id"))
					.groupBy("cell_id").agg(sum("sum_sales").as("sum_sales"));

		// 평균 카드매출액을 scaling 한다.
		rows = DataFrames.scaleMinMax(rows, "sum_sales", "scaled_sum_sales", 0, 1)
						.drop("sum_sales");
		
		return jarvey.toSpatial(rows);
	}

	private static SpatialDataFrame mergeAll(JarveySession jarvey, SpatialDataFrame taxi,
												SpatialDataFrame flowPop, SpatialDataFrame card) {
		Dataset<Row> cardDf = card.toDataFrame();
		Dataset<Row> flowPopDf = flowPop.toDataFrame();
		jarvey.setShufflePartitionCount(1);
		Dataset<Row> rows = cardDf.join(flowPopDf,
										cardDf.col("cell_id").equalTo(flowPopDf.col("cell_id")),
										"fullouter")
									.drop(flowPopDf.col("cell_id"));
		
		rows = rows.na().fill(0.0, new String[]{"scaled_sum_sales", "scaled_avg_pop"});
		rows = rows.withColumn("scaled", expr("scaled_sum_sales + scaled_avg_pop"))
					.select("cell_id", "scaled");
		
		Dataset<Row> taxiDf = taxi.toDataFrame().drop("cell_pos");
		rows = rows.join(taxiDf, rows.col("cell_id").equalTo(taxiDf.col("cell_id")), "fullouter")
					.drop(taxiDf.col("cell_id"));

		rows = rows.na().fill(0.0, new String[]{"scaled", "scaled_visit_count"});
		rows = rows.withColumn("scaled", expr("scaled + scaled_visit_count"))
					.selectExpr("cell_id", "scaled as value");

		return jarvey.toSpatial(rows);
	}
	
	private static SpatialDataFrame attachGeom(JarveySession jarvey, SpatialDataFrame merged,
												SpatialDataFrame blocks) {
		GeometryColumnInfo gcInfo = blocks.assertDefaultGeometryColumnInfo();
		
		Dataset<Row> rows;
		Dataset<Row> blocksDf = blocks.toDataFrame();
		Dataset<Row> mergedDf = merged.toDataFrame();
		rows = blocksDf.repartition(1, blocksDf.col("cell_id"), blocksDf.col("cell_pos"))
						.groupBy("cell_id", "cell_pos")
						.agg(callUDF("ST_Union", col(gcInfo.getName())).as(gcInfo.getName()));
		rows = DataFrames.flattenStruct(rows, true);

		rows = rows.join(mergedDf, "cell_id")
					.select(gcInfo.getName(), "cell_id", "cell_pos", "value");
		
		return jarvey.toSpatial(rows, gcInfo);
	}
	
	static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
}
