package jarvey;

import static jarvey.jarvey_functions.ST_ConvexHull;
import static jarvey.jarvey_functions.ST_CoordDim;
import static jarvey.jarvey_functions.ST_Difference;
import static jarvey.jarvey_functions.ST_EndPoint;
import static jarvey.jarvey_functions.ST_Equals;
import static jarvey.jarvey_functions.ST_ExpandBox;
import static jarvey.jarvey_functions.ST_GeomFromText;
import static jarvey.jarvey_functions.ST_GeomFromWKB;
import static jarvey.jarvey_functions.ST_GeometryN;
import static jarvey.jarvey_functions.ST_Intersection;
import static jarvey.jarvey_functions.ST_Intersects;
import static jarvey.jarvey_functions.ST_IsClosed;
import static jarvey.jarvey_functions.ST_IsRing;
import static jarvey.jarvey_functions.ST_IsSimple;
import static jarvey.jarvey_functions.ST_IsValid;
import static jarvey.jarvey_functions.ST_Relate;
import static jarvey.jarvey_functions.ST_StartPoint;
import static org.apache.spark.sql.functions.array_position;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import utils.Tuple;
import utils.Utilities;
import utils.func.FOption;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;

import jarvey.cluster.QuadSpacePartitioner;
import jarvey.datasource.DatasetOperationException;
import jarvey.datasource.JarveyDataFrameWriter;
import jarvey.optor.AssignUid;
import jarvey.optor.Project;
import jarvey.optor.RDDFunction;
import jarvey.optor.RecordSerDe;
import jarvey.optor.TransformBoxCrs;
import jarvey.optor.geom.Area;
import jarvey.optor.geom.AssignSquareGridCell;
import jarvey.optor.geom.AttachQuadInfo;
import jarvey.optor.geom.Box2d;
import jarvey.optor.geom.BufferTransform;
import jarvey.optor.geom.CentroidTransform;
import jarvey.optor.geom.GeomArgIntersection;
import jarvey.optor.geom.GeomOpOptions;
import jarvey.optor.geom.GeometryPredicate;
import jarvey.optor.geom.ReduceGeometryPrecision;
import jarvey.optor.geom.SquareGrid;
import jarvey.optor.geom.ToPoint;
import jarvey.optor.geom.ToWKB;
import jarvey.optor.geom.ToWKT;
import jarvey.optor.geom.ToXY;
import jarvey.optor.geom.TransformCrs;
import jarvey.optor.geom.join.BroadcastArcClip;
import jarvey.optor.geom.join.BroadcastSpatialDifferenceJoin;
import jarvey.optor.geom.join.BroadcastSpatialInnerJoin;
import jarvey.optor.geom.join.BroadcastSpatialJoin;
import jarvey.optor.geom.join.BroadcastSpatialSemiJoin;
import jarvey.optor.geom.join.CoGroupSpatialInnerJoin;
import jarvey.optor.geom.join.SpatialDataFrameSummary;
import jarvey.optor.geom.join.SpatialJoinOptions;
import jarvey.support.DataFrames;
import jarvey.support.MapTile;
import jarvey.support.RecordLite;
import jarvey.type.EnvelopeType;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;
import jarvey.type.JarveySchemaBuilder;

import scala.Tuple2;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDataFrame implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialDataFrame.class);
	
	private final JarveySession m_jarvey;
	private final JarveySchema m_jschema;
	@Nullable private Dataset<Row> m_df;
	@Nullable private transient JavaRDD<RecordLite> m_jrdd = null;
	
	SpatialDataFrame(JarveySession jarvey, JarveySchema jschema, Dataset<Row> df) {
		this(jarvey, jschema, df, null);
	}
	
	public SpatialDataFrame(JarveySession jarvey, JarveySchema jschema, JavaRDD<RecordLite> jrdd) {
		this(jarvey, jschema, null, jrdd);
	}
	
	private SpatialDataFrame(JarveySession jarvey, JarveySchema jschema, Dataset<Row> df,
							JavaRDD<RecordLite> jrdd) {
		m_jarvey = jarvey;
		m_jschema = jschema;
		m_df = df;
		m_jrdd = jrdd;
	}
	
	public JarveySession getJarveySession() {
		return m_jarvey;
	}
	
	public JarveySchema getJarveySchema() {
		return m_jschema;
	}

	private FOption<Dataset<Row>> getDataFrame() {
		return FOption.ofNullable(m_df);
	}
	public Dataset<Row> toDataFrame() {
		return toDataFrame(true);
	}
	public Dataset<Row> toDataFrame(boolean disableRDD) {
		if ( m_df == null && m_jrdd != null ) {
			RecordSerDe serde = RecordSerDe.of(m_jschema);
			JavaRDD<Row> rows = m_jrdd.map(serde::serialize);
			m_df = m_jarvey.spark().createDataFrame(rows, m_jschema.getSchema());
		}
		if ( disableRDD ) {
			m_jrdd = null;
		}
		
		return m_df;
	}

	private FOption<JavaRDD<RecordLite>> getRecordLiteRDD() {
		return FOption.ofNullable(m_jrdd);
	}
	
	public Partitioner getPartitioner() {
		return m_df.javaRDD().partitioner().orElse(null);
	}
	
	public int getPartitionCount() {
		return m_df.rdd().getNumPartitions();
	}
	
	public int getColumnCount() {
		return m_jschema.getColumnCount();
	}

	public GeometryColumnInfo assertDefaultGeometryColumnInfo() {
		return m_jschema.assertDefaultGeometryColumnInfo();
	}
	public @Nullable GeometryColumnInfo getDefaultGeometryColumnInfo() {
		return m_jschema.getDefaultGeometryColumnInfo();
	}
	
	public @Nullable JarveyColumn getDefaultGeometryColumn() {
		return m_jschema.getDefaultGeometryColumn();
	}
	
	public SpatialDataFrame setDefaultGeometryColumn(String geomCol) {
		return new SpatialDataFrame(m_jarvey, m_jschema.setDefaultGeometryColumn(geomCol), m_df, m_jrdd);
	}
	
	public GeometryColumnInfo getGeometryColumnInfo(String colName) {
		GeometryType colType = m_jschema.getColumn(colName).getJarveyDataType().asGeometryType();
		return new GeometryColumnInfo(colName, colType);
	}
	
	public int getSrid() {
		return FOption.ofNullable(getDefaultGeometryColumnInfo())
						.map(info -> info.getSrid())
						.getOrElse(0);
	}
	
	public StructType schema() {
		return m_jschema.getSchema();
	}
	
	public Column col(String name) {
		return toDataFrame(false).col(name);
	}
	
	public void printSchema() {
		toDataFrame(false).printSchema();
	}
	
	public void explain() {
		toDataFrame(false).explain();
	}
	public void explain(boolean extended) {
		toDataFrame(false).explain(extended);
	}
	public void explain(String mode) {
		toDataFrame(false).explain(mode);
	}

	public void show() {
		toDataFrame(false).show();
	}
	public void show(int numRows) {
		toDataFrame(false).show(numRows);
	}
	public void show(boolean truncate) {
		toDataFrame(false).show(truncate);
	}
	public void show(int numRows, boolean truncate) {
		toDataFrame(false).show(numRows, truncate);
	}
	
	public JarveyDataFrameWriter writeSpatial() {
		return new JarveyDataFrameWriter(this);
	}
	
	public Iterator<Row> toLocalIterator() {
		return toDataFrame(false).toLocalIterator();
	}
	
	public Iterator<RecordLite> toLocalRecordLiteIterator() {
		RecordSerDe serde = RecordSerDe.of(m_jschema);
		return FStream.from(toLocalIterator()).map(serde::deserialize).iterator();
	}
	
	public List<Row> collectAsList() {
		return toDataFrame(false).collectAsList();
	}
	public List<RecordLite> collectAsRecordList() {
		RecordSerDe serde = RecordSerDe.of(m_jschema);
		return FStream.from(collectAsList()).map(serde::deserialize).toList();
	}
	public FStream<RecordLite> streamRecords() {
		return FStream.from(coalesce(1).toLocalRecordLiteIterator());
	}
	
	public long count() {
		return toDataFrame(false).count();
	}
	
	public SpatialDataFrameSummary summarizeSpatialInfo() {
		Dataset<Row> df = toDataFrame(false);
		df = df.agg(callUDF("JV_SummarizeSpatialInfo", col("the_geom")).as("summary"));
		Row row = (Row)df.collectAsList().get(0).get(0);
		long recCount = row.getLong(0);
		long emptyGeomCount = row.getLong(1);
		Envelope bounds = JarveyDataTypes.Envelope_Type.deserialize(row.get(2));
		
		return new SpatialDataFrameSummary(recCount, emptyGeomCount, bounds);
	}
	
	public boolean isEmpty() {
		return toDataFrame(false).isEmpty();
	}
	
	public SpatialDataFrame sample(double fraction) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().sample(fraction));
	}
	
	public SpatialDataFrame as(String alias) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame(false).as(alias));
	}
	
	public SpatialDataFrame cache() {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().cache());
	}
	
	public SpatialDataFrame persist() {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().persist());
	}
	public SpatialDataFrame persist(StorageLevel level) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().persist(level));
	}
	
	public SpatialDataFrame unpersist() {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().unpersist());
	}
	
	public SpatialDataFrame repartition(int nparts, Column... partExpr) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().repartition(nparts, partExpr));
	}
	
	public void cluster(String clusterDsId, long[] qids, boolean force) {
		SpatialDataFrame attacheds = this.attachPartitionQid(qids);

		// repartition 연산에서 실제로 사용하기 위한 partition index를 생성하고 레코드에 추가한다.
		Column partIdx = array_position(lit(qids), attacheds.col(COLUMN_PARTITION_QID)).minus(1).cast("Long");
		attacheds = attacheds.withRegularColumn(COLUMN_PARTITION_INDEX, partIdx);
		
		attacheds.repartition(qids.length+1, attacheds.col(COLUMN_PARTITION_INDEX))
				.writeSpatial()
				.mode(force ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
				.partitionBy(COLUMN_PARTITION_QID)
				.cluster(clusterDsId, qids);
	}

	public SpatialDataFrame select(String... cols) {
		String prjExpr = FStream.of(cols).join(',');
		Project prj = new Project(prjExpr);
		prj.initialize(m_jarvey, m_jschema);
		
		String[] suffix = FStream.of(cols).drop(1).toArray(String.class);
		Dataset<Row> outDf = getDataFrame().map(df -> df.select(cols[0], suffix)).getOrNull();
		JavaRDD<RecordLite> outRdd = getRecordLiteRDD().map(jrdd -> apply(jrdd, prj)).getOrNull();
		return new SpatialDataFrame(m_jarvey, prj.getOutputSchema(), outDf, outRdd);
	}
	public SpatialDataFrame project(String prjExpr) {
		return apply(new Project(prjExpr));
	}
	
	public SpatialDataFrame filter(String expr) {
		Dataset<Row> output = toDataFrame().filter(expr);
		return new SpatialDataFrame(m_jarvey, m_jschema, output);
	}
	public SpatialDataFrame filter(Column col) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().filter(col));
	}
	
	public SpatialDataFrame where(Column col) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().where(col));
	}
	public SpatialDataFrame where(String expr) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().where(expr));
	}
	
	public SpatialDataFrame withColumn(String outCol, Column expr, JarveyDataType jtype) {
		Dataset<Row> df = toDataFrame().withColumn(outCol, expr);

		JarveySchema jschema = m_jschema.toBuilder()
										.addOrReplaceJarveyColumn(outCol, jtype)
										.build();
		return new SpatialDataFrame(m_jarvey, jschema, df);
	}
	public SpatialDataFrame withRegularColumn(String outCol, Column expr) {
		Dataset<Row> df = toDataFrame().withColumn(outCol, expr);
		
		StructType schema = df.schema();
		int idx = schema.fieldIndex(outCol);
		DataType colType = schema.fields()[idx].dataType();
		JarveyDataType jdtype = JarveyDataTypes.fromSparkType(colType);
		
		return withColumn(outCol, expr, jdtype);
	}
	public SpatialDataFrame withEnvelopeColumn(String outCol, Column expr) {
		return withColumn(outCol, expr, JarveyDataTypes.Envelope_Type);
	}
	public SpatialDataFrame withGeometryColumn(String outCol, Column expr) {
		GeometryType geomType = getDefaultGeometryColumnInfo().getDataType();
		return withGeometryColumn(outCol, expr, geomType);
	}
	public SpatialDataFrame withGeometryColumn(String outCol, Column expr, GeometryType geomType) {
		Dataset<Row> df = toDataFrame().withColumn(outCol, expr);
		
		JarveySchemaBuilder builder = m_jschema.toBuilder()
												.addOrReplaceJarveyColumn(outCol, geomType);
		GeometryColumnInfo gcInfo = m_jschema.getDefaultGeometryColumnInfo();
		if ( gcInfo == null ) {
			builder = builder.setDefaultGeometryColumn(outCol);
		}
		JarveySchema jschema = builder.build();
		
		return new SpatialDataFrame(m_jarvey, jschema, df);
	}
	
	public SpatialDataFrame withColumnRenamed(String oldName, String newName) {
		Dataset<Row> df = toDataFrame().withColumnRenamed(oldName, newName);
		JarveySchema schema = m_jschema.rename(oldName, newName);
		return new SpatialDataFrame(m_jarvey, schema, df);
	}
	
	public SpatialDataFrame drop(String colName) {
		return new SpatialDataFrame(m_jarvey, m_jschema.drop(colName), toDataFrame().drop(colName));
	}
	public SpatialDataFrame drop(String... colNames) {
		return new SpatialDataFrame(m_jarvey, m_jschema.drop(colNames), toDataFrame().drop(colNames));
	}
	public SpatialDataFrame drop(Column col) {
		Dataset<Row> df = toDataFrame().drop(col);
		JarveySchema newSchema = m_jschema.select(df.schema().fieldNames());
		return new SpatialDataFrame(m_jarvey, newSchema, df);
	}
	
	public SpatialDataFrame limit(int count) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().limit(count));
	}
	
	public SpatialDataFrame sort(Column... col) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().sort(col));
	}
	
	public SpatialDataFrame coalesce(int nParts) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().coalesce(nParts));
	}
	
	public SpatialDataFrame orderBy(String sortCol, String... sortCols) {
		Dataset<Row> df = toDataFrame();
		Dataset<Row> outDf = df.orderBy(sortCol, sortCols);
		return new SpatialDataFrame(m_jarvey, m_jschema, outDf);
	}
	public SpatialDataFrame orderBy(Column... sortCols) {
		Dataset<Row> df = toDataFrame();
		Dataset<Row> outDf = df.orderBy(sortCols);
		return new SpatialDataFrame(m_jarvey, m_jschema, outDf);
	}
	
	public RelationalGroupedDataset groupBy(Column... cols) {
		Dataset<Row> df = toDataFrame();
		return df.groupBy(cols);
	}
	public RelationalGroupedDataset groupBy(String col, String... cols) {
		Dataset<Row> df = toDataFrame();
		return df.groupBy(col, cols);
	}
	
	public <T> KeyValueGroupedDataset<T,Row> groupByKey(MapFunction<Row,T> func, Encoder<T> enc) {
		Dataset<Row> df = toDataFrame();
		return df.groupByKey(func, enc);
	}
	
	public SpatialDataFrame join(SpatialDataFrame right, Column joinExpr, String type) {
		Dataset<Row> leftDf = toDataFrame();
		Dataset<Row> rightDf = right.toDataFrame();
		
		Dataset<Row> outputDf = leftDf.join(rightDf, joinExpr, type);
		JarveySchema jschema = FStream.from(right.getJarveySchema().getColumnAll())
										.fold(m_jschema.toBuilder(), (b,c) -> b.addJarveyColumn(c))
										.build();
		return new SpatialDataFrame(m_jarvey, jschema, outputDf);
	}
	public SpatialDataFrame join(SpatialDataFrame right, Column joinExpr) {
		Dataset<Row> leftDf = toDataFrame();
		Dataset<Row> rightDf = right.toDataFrame();
		
		Dataset<Row> outputDf = leftDf.join(rightDf, joinExpr);
		JarveySchema jschema = FStream.from(right.getJarveySchema().getColumnAll())
										.fold(m_jschema.toBuilder(), (b,c) -> b.addJarveyColumn(c))
										.build();
		return new SpatialDataFrame(m_jarvey, jschema, outputDf);
	}
	public SpatialDataFrame join(Dataset<?> right, String joinCol) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().join(right, joinCol));
	}
	public SpatialDataFrame join(Dataset<?> right) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().join(right));
	}
	
	public SpatialDataFrame crossJoin(Dataset<?> right) {
		return new SpatialDataFrame(m_jarvey, m_jschema, toDataFrame().crossJoin(right));
	}
	
	public SpatialDataFrame filter(GeometryPredicate pred) {
		return apply(pred);
	}
	
	public SpatialDataFrame area(String outCol) {
		return apply(new Area(outCol));
	}
	
	public SpatialDataFrame box2d(String outCol) {
		return apply(new Box2d(outCol));
	}

	public SpatialDataFrame buffer(double radius, String outCol) {
		GeomOpOptions opts = GeomOpOptions.OUTPUT(outCol);
		return apply(new BufferTransform(radius, opts));
	}
	public SpatialDataFrame buffer(double radius) {
		GeomOpOptions opts = GeomOpOptions.DEFAULT;
		return apply(new BufferTransform(radius, opts));
	}

	public SpatialDataFrame centroid(String outCol) {
		GeomOpOptions opts = GeomOpOptions.OUTPUT(outCol);
		return apply(new CentroidTransform(false, opts));
	}
	public SpatialDataFrame centroid() {
		GeomOpOptions opts = GeomOpOptions.DEFAULT;
		return apply(new CentroidTransform(false, opts));
	}

	public SpatialDataFrame convexhull(String geomCol, String outCol) {
		return withGeometryColumn(outCol, ST_ConvexHull(col(geomCol)));
	}
	public SpatialDataFrame convexhull(String outCol) {
		return convexhull(assertDefaultGeometryColumnName(), outCol);
	}
	public SpatialDataFrame convexhull() {
		return convexhull(assertDefaultGeometryColumnName(), assertDefaultGeometryColumnName());
	}
	
	public SpatialDataFrame reducePrecision(int precisionFactory) {
		GeomOpOptions opts = GeomOpOptions.DEFAULT;
		return apply(new ReduceGeometryPrecision(precisionFactory, opts));
	}
	
	public Column coord_dim(String outCol) {
		return ST_CoordDim(assertDefaultGeometryColumnExpr());
	}

	public SpatialDataFrame end_point(String outCol) {
		return withGeometryColumn(outCol, ST_EndPoint(assertDefaultGeometryColumnExpr()));
	}
	public SpatialDataFrame end_point() {
		return end_point(assertDefaultGeometryColumnName());
	}
	
	public SpatialDataFrame expand_box(String boxCol, double dist, String outCol) {
		return withColumn(outCol, ST_ExpandBox(col(boxCol), lit(dist)), EnvelopeType.get());
	}
	public SpatialDataFrame expand_box(String boxCol, double dx, double dy, String outCol) {
		return withColumn(outCol, ST_ExpandBox(col(boxCol), lit(dx), lit(dy)), EnvelopeType.get());
	}
	
	public SpatialDataFrame from_wkt(Column wkt, int srid, String outCol) {
		GeometryType colType = GeometryType.of(Geometries.GEOMETRY, srid);
		SpatialDataFrame sds = withColumn(outCol, ST_GeomFromText(wkt), colType);
		if ( sds.getDefaultGeometryColumnInfo() == null ) {
			sds = sds.setDefaultGeometryColumn(outCol);
		}
		return sds;
	}
	
	public SpatialDataFrame from_wkb(Column wkb, int srid, String outCol) {
		GeometryType colType = GeometryType.of(Geometries.GEOMETRY, srid);
		SpatialDataFrame sds = withColumn(outCol, ST_GeomFromWKB(wkb), colType);
		if ( sds.getDefaultGeometryColumnInfo() == null ) {
			sds = sds.setDefaultGeometryColumn(outCol);
		}
		return sds;
	}

	public SpatialDataFrame geometry_at(String geomCol, int index, String outCol) {
		return withGeometryColumn(outCol, ST_GeometryN(col(geomCol), index));
	}
	public SpatialDataFrame geometry_at(int index, String outCol) {
		return geometry_at(assertDefaultGeometryColumnName(), index, outCol);
	}
	public SpatialDataFrame geometry_at(int index) {
		return geometry_at(assertDefaultGeometryColumnName(), index, assertDefaultGeometryColumnName());
	}
		
	public Column intersects_with(String rightCol) {
		return ST_Intersects(assertDefaultGeometryColumnExpr(), col(rightCol));
	}
	public Column intersects_with(Geometry geom) {
		return ST_Intersects(assertDefaultGeometryColumnExpr(), geom);
	}
	public Column intersects_with(Envelope envl) {
		return intersects_with(GeometryUtils.toPolygon(envl));
	}
	
	public Column equals_to(String rightGeomCol) {
		return ST_Equals(assertDefaultGeometryColumnExpr(), col(rightGeomCol));
	}
	public Column equals_to(Geometry geom) {
		return ST_Equals(assertDefaultGeometryColumnExpr(), geom);
	}
	
	public SpatialDataFrame intersection_with(Geometry paramGeom, String outCol) {
		return apply(new GeomArgIntersection(paramGeom, GeomOpOptions.OUTPUT(outCol)));
	}
	public SpatialDataFrame intersection_with(Geometry paramGeom) {
		return apply(new GeomArgIntersection(paramGeom, GeomOpOptions.DEFAULT));
	}
	
	public SpatialDataFrame intersection_with(String rightGeomCol, String outCol) {
		assertCompatibleSrid("intersection", assertDefaultGeometryColumnName(), rightGeomCol);
		return withGeometryColumn(outCol, ST_Intersection(assertDefaultGeometryColumnExpr(), col(rightGeomCol)));
	}
	public SpatialDataFrame intersection_with(String rightGeomCol) {
		return intersection_with(rightGeomCol, assertDefaultGeometryColumnName());
	}

	public SpatialDataFrame difference_from(String rightGeomCol, String outCol) {
		assertCompatibleSrid("difference", assertDefaultGeometryColumnName(), rightGeomCol);

		return withGeometryColumn(outCol, ST_Difference(assertDefaultGeometryColumnExpr(), col(rightGeomCol)));
	}
	public SpatialDataFrame difference_from(String rightGeomCol) {
		return difference_from(rightGeomCol, assertDefaultGeometryColumnName());
	}
	
	public Column is_closed() {
		return ST_IsClosed(assertDefaultGeometryColumnExpr());
	}
	public Column is_ring() {
		return ST_IsRing(assertDefaultGeometryColumnExpr());
	}
	public Column is_simple() {
		return ST_IsSimple(assertDefaultGeometryColumnExpr());
	}
	public Column is_valid() {
		return ST_IsValid(assertDefaultGeometryColumnExpr());
	}

	public Column num_geometries(String outCol) {
		return callUDF("ST_NumGeometries", assertDefaultGeometryColumnExpr());
	}
	
	public SpatialDataFrame point(String xCol, String yCol, int srid, String outCol) {
		return apply(new ToPoint(xCol, yCol, srid, outCol));
	}

	public Column relate_with(String rightGeomCol, String pattern) {
		return ST_Relate(assertDefaultGeometryColumnExpr(), col(rightGeomCol), lit(pattern));
	}
	public Column relate_pattern(String rightGeomCol) {
		return ST_Relate(assertDefaultGeometryColumnExpr(), col(rightGeomCol));
	}

	public SpatialDataFrame set_srid(int srid) {
		GeometryColumnInfo geomColInfo = m_jschema.assertDefaultGeometryColumnInfo();
		GeometryType newType = GeometryType.of(geomColInfo.getGeometries(), srid);
		JarveySchema newSchema = m_jschema.update(geomColInfo.getName(), jcol -> {
			return new JarveyColumn(jcol.getIndex(), jcol.getName(), newType);
		});
		
		return new SpatialDataFrame(m_jarvey, newSchema, toDataFrame());
	}
	
	public SpatialDataFrame srid(String geomCol, String outCol) {
		int srid = m_jschema.getColumn(geomCol).getJarveyDataType().asGeometryType().getSrid();
		return withRegularColumn(outCol, lit(srid));
	}
	public SpatialDataFrame srid(String outCol) {
		return srid(assertDefaultGeometryColumnName(), outCol);
	}

	public SpatialDataFrame start_point(String outCol) {
		return withGeometryColumn(outCol, ST_StartPoint(assertDefaultGeometryColumnExpr()));
	}
	public SpatialDataFrame start_point() {
		return start_point(assertDefaultGeometryColumnName());
	}

	public SpatialDataFrame to_wkb(String outCol) {
		return apply(new ToWKB(outCol));
	}

	public SpatialDataFrame to_wkt(String outCol) {
		return apply(new ToWKT(outCol));
	}

	public SpatialDataFrame transformCrs(int toSrid, String outCol) {
		GeometryColumnInfo gcInfo = m_jschema.getDefaultGeometryColumnInfo();
		
		if ( gcInfo.getSrid() == toSrid ) {
			String inputGeomColName = assertDefaultGeometryColumnName();
			String outputGeomColName = FOption.getOrElse(outCol, inputGeomColName);
			if ( !inputGeomColName.equals(outputGeomColName) ) {
				return withGeometryColumn(outputGeomColName, col(inputGeomColName));
			}
			else {
				return this;
			}
		}
		else {
			GeomOpOptions opts = (outCol != null) ? GeomOpOptions.OUTPUT(outCol) : GeomOpOptions.DEFAULT;
			return apply(new TransformCrs(toSrid, opts));
		}
	}
	public SpatialDataFrame transformCrs(int toSrid) {
		return transformCrs(toSrid, null);
	}

	public SpatialDataFrame transform_box(String boxCol, int toSrid, String outCol) {
		GeometryColumnInfo gcInfo = getDefaultGeometryColumnInfo();
		
		if ( gcInfo.getSrid() != toSrid ) {
			return apply(new TransformBoxCrs(boxCol, outCol, gcInfo.getSrid(), toSrid));
		}
		else if ( !boxCol.equals(outCol) ) {
			return withColumn(outCol, col(boxCol), EnvelopeType.get());
		}
		else {
			return this;
		}
	}
	public SpatialDataFrame transform_box(String boxCol, int toSrid) {
		return transform_box(boxCol, toSrid, boxCol);
	}
	
	public SpatialDataFrame to_xy(String xCol, String yCol) {
		return apply(new ToXY(xCol, yCol));
	}
	
	public SpatialDataFrame assignUid(String outCol) {
		return apply(new AssignUid(outCol));
	}
	
	public SpatialDataFrame toVector(String[] cols, String outCol) {
		return m_jarvey.toSpatial(DataFrames.toVector(toDataFrame(), cols, outCol),
									getDefaultGeometryColumnInfo());
	}
	public SpatialDataFrame toVector(String arrayCol, String outCol) {
		return m_jarvey.toSpatial(DataFrames.toVector(toDataFrame(), arrayCol, outCol),
									getDefaultGeometryColumnInfo());
	}
	public SpatialDataFrame toDoubleArray(String vectorCol, String outCols) {
		return m_jarvey.toSpatial(DataFrames.toDoubleArray(toDataFrame(), vectorCol, outCols),
									getDefaultGeometryColumnInfo());
	}
	public SpatialDataFrame toFloatArray(String vectorCol, String outCols) {
		return m_jarvey.toSpatial(DataFrames.toFloatArray(toDataFrame(), vectorCol, outCols),
									getDefaultGeometryColumnInfo());
	}
	public SpatialDataFrame toArray(String[] cols, String outCol) {
		return m_jarvey.toSpatial(DataFrames.toArray(toDataFrame(), cols, outCol),
									getDefaultGeometryColumnInfo());
	}
	public SpatialDataFrame toMultiColumns(String arrayCol, String[] outCols) {
		return m_jarvey.toSpatial(DataFrames.toMultiColumns(toDataFrame(), arrayCol, outCols),
									getDefaultGeometryColumnInfo());
	}

	public static final String COLUMN_ENVL4326 = "_QUAD_ENVL";
	public static final String COLUMN_PARTITION_QID = "_PARTITION_QID";
	public static final String COLUMN_PARTITION_INDEX = "_PARTITION_INDEX";
	public static final String COLUMN_QUAD_IDS = "_QUAD_IDS";
	public SpatialDataFrame attachQuadInfo(long[] candidates) {
		return apply(new AttachQuadInfo(candidates));
	}
	
	public SpatialDataFrame agg(Column expr, Column... exprs) {
		Dataset<Row> df = toDataFrame();
		Dataset<Row> outDf = df.agg(expr, exprs);
		
		JarveySchemaBuilder builder = JarveySchema.builder();
		for ( StructField field: outDf.schema().fields() ) {
			if ( field.dataType() instanceof StructType ) {
				Tuple<JarveyDataType,Dataset<Row>> unwrapped = JarveyDataTypes.unwrap(field, outDf);
				if ( unwrapped != null ) {
					builder.addJarveyColumn(field.name(), unwrapped._1);
					outDf = unwrapped._2;
				}
			}
			else {
				builder.addJarveyColumn(field.name(), JarveyDataTypes.fromSparkType(field.dataType()));
			}
		}
		JarveySchema jschema = builder.build();
		return new SpatialDataFrame(m_jarvey, jschema, outDf);
	}
	
	public SpatialDataFrame spatialBroadcastJoin(SpatialDataFrame inner, SpatialJoinOptions jopts) {
		List<Row> rows = inner.collectAsList();
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("broadcasting {} rows", rows.size());
		}
		
		Broadcast<List<Row>> broadcast = m_jarvey.javaSc().broadcast(rows);
		return spatialBroadcastJoin(inner.getJarveySchema(), broadcast, jopts);
	}
	
	public SpatialDataFrame spatialBroadcastJoin(JarveySchema innerSchema, Broadcast<List<Row>> inner,
													SpatialJoinOptions jopts) {
		BroadcastSpatialJoin joinOp;
		switch ( jopts.joinType() ) {
			case SpatialJoinOptions.INNER_JOIN:
				joinOp = new BroadcastSpatialInnerJoin(innerSchema, inner, jopts);
				break;
			case SpatialJoinOptions.SEMI_JOIN:
				joinOp = new BroadcastSpatialSemiJoin(innerSchema, inner, jopts);
				break;
			case SpatialJoinOptions.DIFFERENCE_JOIN:
				joinOp = new BroadcastSpatialDifferenceJoin(innerSchema, inner, jopts);
				break;
			case SpatialJoinOptions.ARC_CLIP:
				joinOp = new BroadcastArcClip(innerSchema, inner, jopts);
				break;
			default:
				throw new IllegalArgumentException("invalid broadcast join type: " + jopts.joinType());
		}
		return apply(joinOp);
	}

	public SpatialDataFrame spatialJoinRDD(SpatialDataFrame right, SpatialJoinOptions opts,
											QuadSpacePartitioner partitioner) {
		Tuple<JarveySchema,JavaPairRDD<Long,Row>> leftTup = this.partitionByQuadSpaces(partitioner);
		Tuple<JarveySchema,JavaPairRDD<Long,Row>> rightTup = right.partitionByQuadSpaces(partitioner);
		
		JavaPairRDD<Long,Row> leftPairs = leftTup._2;
		JavaPairRDD<Long,Row> rightPairs = rightTup._2;
		JavaPairRDD<Long,Tuple2<Iterable<Row>,Iterable<Row>>> grouped = leftPairs.cogroup(rightPairs);
		
		JarveySchema leftPairSchema = leftTup._1;
		JarveySchema rightPairSchema = rightTup._1;
		
		jarvey.junk.CoGroupSpatialJoin joinFunc = null;
		switch (opts.joinType()) {
			case SpatialJoinOptions.INNER_JOIN:
				joinFunc = new jarvey.junk.CoGroupSpatialInnerJoin(leftPairSchema, rightPairSchema, opts);
				break;
		}
		JavaRDD<Row> joined = grouped.flatMap(joinFunc);
		
		JarveySchema jschema = joinFunc.getOutputSchema();
		Dataset<Row> df = m_jarvey.spark().createDataFrame(joined, jschema.getSchema());
		return new SpatialDataFrame(m_jarvey, jschema, df);
	}
	
	private SpatialDataFrame attachPartitionQid(long[] qids) {
		SpatialDataFrame attacheds = attachQuadInfo(qids);
		attacheds = attacheds.withRegularColumn(COLUMN_PARTITION_QID, explode(attacheds.col(COLUMN_QUAD_IDS)));

		// repartition 연산에서 실제로 사용하기 위한 partition index를 생성하고 레코드에 추가한다.
		Column partIdx = array_position(lit(qids), attacheds.col(COLUMN_PARTITION_QID)).minus(1).cast("Long");
		attacheds = attacheds.withRegularColumn(COLUMN_PARTITION_INDEX, partIdx);
		
		return attacheds;
	}
	private static final MapFunction<Row, Long> GetPartitionQid = (row) -> row.getAs(COLUMN_PARTITION_QID);
	public SpatialDataFrame spatialJoin(SpatialDataFrame right, SpatialJoinOptions opts, long[] quadIds) {
		Set<Long> qidSet = Sets.newTreeSet(Longs.asList(quadIds));
		qidSet.add(MapTile.OUTLIER_QID);
		// qid를 기준으로 sort하기 때문에 outlier quid (-9)가 0번째 위치하게 됨.
		quadIds = FStream.from(qidSet).mapToLong(v -> v).toArray();
		
		SpatialDataFrame attachedLeft = attachPartitionQid(quadIds);
		KeyValueGroupedDataset<Long,Row> kvLeft = attachedLeft.groupByKey(GetPartitionQid, Encoders.LONG());
		
		SpatialDataFrame attachedRight = right.attachPartitionQid(quadIds);
		KeyValueGroupedDataset<Long,Row> kvRight = attachedRight.groupByKey(GetPartitionQid, Encoders.LONG());
		
		CoGroupSpatialInnerJoin joinFunc;
		switch ( opts.joinType() ) {
			case SpatialJoinOptions.INNER_JOIN:
				joinFunc = new CoGroupSpatialInnerJoin(attachedLeft, attachedRight, opts);
				break;
			default:
				throw new IllegalArgumentException("unsupported spatial-join type: " + opts.joinType());
		}
		JarveySchema outputSchema = joinFunc.getOutputSchema();
		
		Dataset<Row> joined = kvLeft.cogroup(kvRight, joinFunc, RowEncoder.apply(outputSchema.getSchema()));
		return new SpatialDataFrame(m_jarvey, outputSchema, joined);
	}
	
	/**
	 * 입력 레코드 세트에 포함된 각 레코드에 주어진 크기의 사각형 Grid 셀 정보를 부여한다.
	 * 
	 * 출력 레코드에는 '{@code cell_id}', '{@code cell_pos}', 그리고 '{@code cell_geom}'
	 * 컬럼이 추가된다. 각 컬럼 내용은 각각 다음과 같다.
	 * <dl>
	 * 	<dt>cell_id</dt>
	 * 	<dd>부여된 그리드 셀의 고유 식별자. {@code long} 타입</dd>
	 * 	<dt>cell_pos</dt>
	 * 	<dd>부여된 그리드 셀의 x/y 좌표 . {@code GridCell} 타입</dd>
	 * 	<dt>cell_geom</dt>
	 * 	<dd>부여된 그리드 셀의 공간 객체 . {@code Polygon} 타입</dd>
	 * </dl>
	 * 
	 * 입력 레코드의 공간 객체가 {@code null}이거나 {@link Geometry#isEmpty()}가
	 * {@code true}인 경우, 또는 공간 객체의 위치가 그리드 전체 영역 밖에 있는 레코드의
	 * 처리는 {@code ignoreOutside} 인자에 따라 처리된다.
	 * 만일 {@code ignoreOutside}가 {@code false}인 경우 영역 밖 레코드의 경우는
	 * 출력 레코드 세트에 포함되지만, '{@code cell_id}', '{@code cell_pos}',
	 * '{@code cell_geom}'의 컬럼 값은 {@code null}로 채워진다.
	 * 
	 * @param geomCol	Grid cell 생성에 사용할 공간 컬럼 이름.
	 * @param grid		생성할 격자 정보.
	 * @param assignOutside	입력 공간 객체가 주어진 그리드 전체 영역에서 벗어난 경우
	 * 						무시 여부. 무시하는 경우는 결과 레코드 세트에 포함되지 않음.
	 * @return	명령이 추가된 {@link PlanBuilder} 객체.
	 */
	public SpatialDataFrame assignGridCell(SquareGrid grid, boolean assignOutside) {
		Utilities.checkNotNullArgument(grid, "SquareGrid is null");

		return apply(new AssignSquareGridCell(grid, assignOutside));
	}
	
	public Tuple<JarveySchema, JavaPairRDD<Long,Row>> partitionByQuadSpaces(QuadSpacePartitioner partitioner) {
		SpatialDataFrame attacheds = attachPartitionQid(partitioner.getQuadIds());

		JarveySchema jschema = attacheds.getJarveySchema();
		int cidx = jschema.getColumn(COLUMN_PARTITION_QID).getIndex();
		
		// 'COLUMN_PARTITION_QID' 컬럼 값을 key로하는 PairRDD를 생성한다. 
		JavaPairRDD<Long,Row> pairs = attacheds.toDataFrame().javaRDD()
												.mapToPair(r -> new Tuple2<>(r.getLong(cidx), r));
		
		// 'COLUMN_PARTITION_QID' 컬럼 값을 key로 RDD를 partition들로 나눈다.
		JavaPairRDD<Long,Row> partitioneds = pairs.partitionBy(partitioner);
		
		return Tuple.of(jschema, partitioneds);
	}

	private JavaRDD<RecordLite> apply(JavaRDD<RecordLite> input, RDDFunction func) {
		func.initialize(m_jarvey, m_jschema);
		return func.apply(input);
	}
	public SpatialDataFrame apply(RDDFunction func) {
		// 연산자를 초기화 시킨다.
		func.initialize(m_jarvey, m_jschema);
		
		JavaRDD<RecordLite> input = m_jrdd;
		if ( input == null ) {
			JavaRDD<Row> rows = m_df.rdd().toJavaRDD();
			
			RecordSerDe serde = RecordSerDe.of(m_jschema);
			input = rows.map(serde::deserialize);
		}
		
		// 연산을 적용하여 출력 RDD를 생성함.
		JavaRDD<RecordLite> output = func.apply(input);
		
		return new SpatialDataFrame(m_jarvey, func.getOutputSchema(), output);
	}

	private void assertCompatibleSrid(String op, String geomCol1, String geomCol2) {
		int srid1 = m_jschema.getColumn(geomCol1).getJarveyDataType().asGeometryType().getSrid();
		int srid2 = m_jschema.getColumn(geomCol2).getJarveyDataType().asGeometryType().getSrid();
		
		if ( srid1 > 0 && srid2 > 0 && srid1 != srid2 ) {
			throw new DatasetOperationException(String.format("incompatible SRID: op=%s(%s(%s), %s(%s))",
															op, geomCol1, srid1, geomCol2, srid2));
		}
	}
	
	private Column assertDefaultGeometryColumnExpr() {
		return col(assertDefaultGeometryColumnName());
	}
	
	private String assertDefaultGeometryColumnName() {
		return assertDefaultGeometryColumn().getName().get();
	}
	
	private JarveyColumn assertDefaultGeometryColumn() {
		JarveyColumn jcol = m_jschema.getDefaultGeometryColumn();
		if ( jcol == null ) {
			throw new IllegalStateException("Default Geometry column is not found");
		}
		
		return jcol;
	}
}
