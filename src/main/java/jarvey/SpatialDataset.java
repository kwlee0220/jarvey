/**
 * 
 */
package jarvey;

import static jarvey.jarvey_functions.JV_AttachQuadMembers;
import static jarvey.jarvey_functions.ST_AsBinary;
import static jarvey.jarvey_functions.ST_AsText;
import static jarvey.jarvey_functions.ST_Box2d;
import static jarvey.jarvey_functions.ST_BoxIntersects;
import static jarvey.jarvey_functions.ST_Buffer;
import static jarvey.jarvey_functions.ST_Centroid;
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
import static jarvey.jarvey_functions.ST_Transform;
import static jarvey.jarvey_functions.ST_TransformBox;
import static jarvey.jarvey_functions.ST_X;
import static jarvey.jarvey_functions.ST_Y;
import static jarvey.jarvey_functions.ST_Point;
import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;

import javax.annotation.Nullable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import jarvey.datasource.DatasetOperationException;
import jarvey.datasource.JarveyDataFrameWriter;
import jarvey.join.CoGroupDifferenceJoin;
import jarvey.join.CoGroupSemiSpatialJoin;
import jarvey.join.CoGroupSpatialBlockJoin;
import jarvey.join.SpatialJoinOptions;
import jarvey.support.MapTile;
import jarvey.support.colexpr.ColumnSelector;
import jarvey.type.ArrayType;
import jarvey.type.EnvelopeType;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;
import utils.geo.util.CoordinateTransform;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;
import utils.stream.LongFStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialDataset extends Dataset<Row> {
	private static final long serialVersionUID = 1L;
	
	private final JarveySession m_jarvey;
	private final Dataset<Row> m_df;
	private final JarveySchema m_jarveySchema;
	
	public SpatialDataset(JarveySession jarvey, Dataset<Row> df, JarveySchema jschema) {
		super(df.sparkSession(), df.logicalPlan(), df.encoder());
		
		m_jarvey = jarvey;
		m_df = df;
		m_jarveySchema = jschema;
	}
	
	public JarveySession getJarveySession() {
		return m_jarvey;
	}
	
	public Dataset<Row> getDataFrame() {
		return m_df;
	}
	
	public JarveySchema getJarveySchema() {
		return m_jarveySchema;
	}
	
	public @Nullable GeometryColumnInfo getDefaultGeometryColumnInfo() {
		return m_jarveySchema.getDefaultGeometryColumnInfo();
	}
	
	public @Nullable JarveyColumn getDefaultGeometryColumn() {
		return m_jarveySchema.getDefaultGeometryColumn();
	}
	
	public SpatialDataset setDefaultGeometryColumn(String geomCol) {
		return new SpatialDataset(m_jarvey, m_df, m_jarveySchema.setDefaultGeometryColumn(geomCol));
	}
	
	public GeometryColumnInfo getGeometryColumnInfo(String colName) {
		int srid = m_jarveySchema.getColumn(colName).getJarveyDataType().asGeometryType().getSrid();
		return new GeometryColumnInfo(colName, srid);
	}
	
	public void printSchema() {
		m_df.printSchema();
	}
	
	public JarveyDataFrameWriter writeSpatial() {
		return new JarveyDataFrameWriter(this);
	}
	
	public SpatialDataset sample(double fraction) {
		return new SpatialDataset(m_jarvey, m_df.sample(fraction), m_jarveySchema);
	}
	
	public SpatialDataset as(String alias) {
		return new SpatialDataset(m_jarvey, m_df.as(alias), m_jarveySchema);
	}
	
	public SpatialDataset cache() {
		return new SpatialDataset(m_jarvey, m_df.cache(), m_jarveySchema);
	}
	
	public SpatialDataset persist() {
		return new SpatialDataset(m_jarvey, m_df.persist(), m_jarveySchema);
	}
	public SpatialDataset persist(StorageLevel level) {
		return new SpatialDataset(m_jarvey, m_df.persist(level), m_jarveySchema);
	}
	
	public SpatialDataset repartition(int nparts, Column... partExpr) {
		return new SpatialDataset(m_jarvey, m_df.repartition(nparts, partExpr), m_jarveySchema);
	}
	
	public void cluster(String clusterDsId, Long[] qids, boolean force) {
		SpatialDataset attacheds = this.attachQuadIds(qids);
		
		attacheds.repartition(qids.length+1, attacheds.col(CLUSTER_ID))
				.writeSpatial()
				.mode(force ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
				.bucketBy(qids.length+1, CLUSTER_ID)
				.dataset(clusterDsId, qids);
//		attacheds.repartition(qids.length+1, attacheds.col(CLUSTER_ID))
//				.writeSpatial()
//				.mode(opts.force() ? SaveMode.Overwrite : SaveMode.ErrorIfExists)
//				.partitionBy(CLUSTER_ID)
//				.dataset(clusterDsId, qids);
	}
	
	public SpatialDataset select(Column... cols) {
		Dataset<Row> df = m_df.select(cols);
		JarveySchema jschema = m_jarveySchema.select(df.schema().fieldNames());
		return new SpatialDataset(m_jarvey, df, jschema);
	}
	
	public SpatialDataset select(String col, String... cols) {
		Dataset<Row> df = m_df.select(col, cols);
		JarveySchema jschema = m_jarveySchema.select(df.schema().fieldNames());
		return new SpatialDataset(m_jarvey, df, jschema);
	}
	
	public SpatialDataset selectExpr(String expr) {
		Column[] cols = ColumnSelector.fromExpression(expr).addOrReplaceDataset("", this).select();
		return select(cols);
	}
	
	public SpatialDataset filter(String expr) {
		return new SpatialDataset(m_jarvey, m_df.filter(expr), m_jarveySchema);
	}
	public SpatialDataset filter(Column col) {
		return new SpatialDataset(m_jarvey, m_df.filter(col), m_jarveySchema);
	}
	
	public SpatialDataset where(Column col) {
		return new SpatialDataset(m_jarvey, m_df.where(col), m_jarveySchema);
	}
	public SpatialDataset where(String expr) {
		return new SpatialDataset(m_jarvey, m_df.where(expr), m_jarveySchema);
	}

	public SpatialDataset withColumn(String outCol, Column expr, JarveyDataType jtype) {
		Dataset<Row> df = m_df.withColumn(outCol, expr);
		JarveySchema jschema = m_jarveySchema.updateColumn(df.schema(), outCol, jtype);
		return new SpatialDataset(m_jarvey, df, jschema);
	}
	public SpatialDataset withRegularColumn(String outCol, Column expr) {
		Dataset<Row> df = m_df.withColumn(outCol, expr);
		JarveySchema jschema = m_jarveySchema.updateRegularColumn(df.schema(), outCol);
		return new SpatialDataset(m_jarvey, df, jschema);
	}
	public SpatialDataset withGeometryColumn(String outCol, Column expr) {
		JarveyDataType jtype = getDefaultGeometryColumn().getJarveyDataType();
		return withColumn(outCol, expr, jtype);
	}
	public SpatialDataset withColumnRenamed(String oldName, String newName) {
		Dataset<Row> df = m_df.withColumnRenamed(oldName, newName);
		JarveySchema schema = m_jarveySchema.rename(oldName, newName);
		return new SpatialDataset(m_jarvey, df, schema);
	}
	
	public SpatialDataset drop(String colName) {
		return new SpatialDataset(m_jarvey, m_df.drop(colName), m_jarveySchema.drop(colName));
	}
	public SpatialDataset drop(String... colNames) {
		return new SpatialDataset(m_jarvey, m_df.drop(colNames), m_jarveySchema.drop(colNames));
	}
	public SpatialDataset drop(Column col) {
		Dataset<Row> df = m_df.drop(col);
		JarveySchema newSchema = m_jarveySchema.select(df.schema().fieldNames());
		return new SpatialDataset(m_jarvey, df, newSchema);
	}
	
	public SpatialDataset limit(int count) {
		return new SpatialDataset(m_jarvey, m_df.limit(count), m_jarveySchema);
	}
	
	public SpatialDataset coalesce(int nParts) {
		return new SpatialDataset(m_jarvey, m_df.coalesce(nParts), m_jarveySchema);
	}
	
	public SpatialDataset join(Dataset<?> right, Column joinExpr, String type) {
		return new SpatialDataset(m_jarvey, m_df.join(right, joinExpr, type), m_jarveySchema);
	}
	public SpatialDataset join(Dataset<?> right, Column joinExpr) {
		return new SpatialDataset(m_jarvey, m_df.join(right, joinExpr), m_jarveySchema);
	}
	public SpatialDataset join(Dataset<?> right, String joinCol) {
		return new SpatialDataset(m_jarvey, m_df.join(right, joinCol), m_jarveySchema);
	}
	public SpatialDataset join(Dataset<?> right) {
		return new SpatialDataset(m_jarvey, m_df.join(right), m_jarveySchema);
	}
	
	public SpatialDataset crossJoin(Dataset<?> right) {
		return new SpatialDataset(m_jarvey, m_df.crossJoin(right), m_jarveySchema);
	}
	
	public SpatialDataset box2d(String outCol) {
		return withColumn(outCol, ST_Box2d(getDefaultGeometryColumnExpr()), EnvelopeType.get());
	}

	public SpatialDataset buffer(double radius, String outCol) {
		return withGeometryColumn(outCol, ST_Buffer(getDefaultGeometryColumnExpr(), radius));
	}
	public SpatialDataset buffer(double radius) {
		return buffer(radius, getDefaultGeometryColumnName());
	}

	public SpatialDataset centroid(String outCol) {
		return withGeometryColumn(outCol, ST_Centroid(getDefaultGeometryColumnExpr()));
	}
	public SpatialDataset centroid() {
		return centroid(getDefaultGeometryColumnName());
	}

	public SpatialDataset convexhull(String geomCol, String outCol) {
		return withGeometryColumn(outCol, ST_ConvexHull(col(geomCol)));
	}
	public SpatialDataset convexhull(String outCol) {
		return convexhull(getDefaultGeometryColumnName(), outCol);
	}
	public SpatialDataset convexhull() {
		return convexhull(getDefaultGeometryColumnName(), getDefaultGeometryColumnName());
	}
	
	public Column coord_dim(String outCol) {
		return ST_CoordDim(getDefaultGeometryColumnExpr());
	}

	public SpatialDataset end_point(String outCol) {
		return withGeometryColumn(outCol, ST_EndPoint(getDefaultGeometryColumnExpr()));
	}
	public SpatialDataset end_point() {
		return end_point(getDefaultGeometryColumnName());
	}
	
	public SpatialDataset expand_box(String boxCol, double dist, String outCol) {
		return withColumn(outCol, ST_ExpandBox(col(boxCol), lit(dist)), EnvelopeType.get());
	}
	public SpatialDataset expand_box(String boxCol, double dx, double dy, String outCol) {
		return withColumn(outCol, ST_ExpandBox(col(boxCol), lit(dx), lit(dy)), EnvelopeType.get());
	}
	
	public SpatialDataset from_wkt(Column wkt, int srid, String outCol) {
		SpatialDataset sds = withColumn(outCol, ST_GeomFromText(wkt), GeometryType.of(srid));
		if ( sds.getDefaultGeometryColumnInfo() == null ) {
			sds = sds.setDefaultGeometryColumn(outCol);
		}
		return sds;
	}
	
	public SpatialDataset from_wkb(Column wkb, int srid, String outCol) {
		SpatialDataset sds = withColumn(outCol, ST_GeomFromWKB(wkb), GeometryType.of(srid));
		if ( sds.getDefaultGeometryColumnInfo() == null ) {
			sds = sds.setDefaultGeometryColumn(outCol);
		}
		return sds;
	}

	public SpatialDataset geometry_at(String geomCol, int index, String outCol) {
		return withGeometryColumn(outCol, ST_GeometryN(col(geomCol), index));
	}
	public SpatialDataset geometry_at(int index, String outCol) {
		return geometry_at(getDefaultGeometryColumnName(), index, outCol);
	}
	public SpatialDataset geometry_at(int index) {
		return geometry_at(getDefaultGeometryColumnName(), index, getDefaultGeometryColumnName());
	}
		
	public Column intersects_with(String rightCol) {
		return ST_Intersects(getDefaultGeometryColumnExpr(), col(rightCol));
	}
	public Column intersects_with(Geometry geom) {
		return ST_Intersects(getDefaultGeometryColumnExpr(), geom);
	}
	public Column intersects_with(Envelope envl) {
		return intersects_with(GeometryUtils.toPolygon(envl));
	}
	
	public Column equals_to(String rightGeomCol) {
		return ST_Equals(getDefaultGeometryColumnExpr(), col(rightGeomCol));
	}
	public Column equals_to(Geometry geom) {
		return ST_Equals(getDefaultGeometryColumnExpr(), geom);
	}
	
	public SpatialDataset intersection_with(String rightGeomCol, String outCol) {
		assertCompatibleSrid("intersection", getDefaultGeometryColumnName(), rightGeomCol);
		return withGeometryColumn(outCol, ST_Intersection(getDefaultGeometryColumnExpr(), col(rightGeomCol)));
	}
	public SpatialDataset intersection_with(String rightGeomCol) {
		return intersection_with(rightGeomCol, getDefaultGeometryColumnName());
	}

	public SpatialDataset difference_from(String rightGeomCol, String outCol) {
		assertCompatibleSrid("difference", getDefaultGeometryColumnName(), rightGeomCol);

		return withGeometryColumn(outCol, ST_Difference(getDefaultGeometryColumnExpr(), col(rightGeomCol)));
	}
	public SpatialDataset difference_from(String rightGeomCol) {
		return difference_from(rightGeomCol, getDefaultGeometryColumnName());
	}
	
	public Column is_closed() {
		return ST_IsClosed(getDefaultGeometryColumnExpr());
	}
	public Column is_ring() {
		return ST_IsRing(getDefaultGeometryColumnExpr());
	}
	public Column is_simple() {
		return ST_IsSimple(getDefaultGeometryColumnExpr());
	}
	public Column is_valid() {
		return ST_IsValid(getDefaultGeometryColumnExpr());
	}

	public Column num_geometries(String outCol) {
		return callUDF("ST_NumGeometries", getDefaultGeometryColumnExpr());
	}
	
	public SpatialDataset point(String xCol, String yCol, int srid, String outCol) {
		return withColumn(outCol, ST_Point(col(xCol), col(yCol)), GeometryType.of(srid));
	}

	public Column relate_with(String rightGeomCol, String pattern) {
		return ST_Relate(getDefaultGeometryColumnExpr(), col(rightGeomCol), lit(pattern));
	}
	public Column relate_pattern(String rightGeomCol) {
		return ST_Relate(getDefaultGeometryColumnExpr(), col(rightGeomCol));
	}

	private SpatialDataset set_srid(String geomCol, int srid, String outCol) {
		if ( !geomCol.equals(outCol) ) {
			return withColumn(outCol, col(geomCol), GeometryType.of(srid));
		}
		else {
			JarveySchema jschema = m_jarveySchema.updateColumn(m_df.schema(), outCol, GeometryType.of(srid));
			return new SpatialDataset(m_jarvey, m_df, jschema);
		}
	}
	public SpatialDataset set_srid(int srid, String outCol) {
		return set_srid(getDefaultGeometryColumnName(), srid, outCol);
	}
	public SpatialDataset set_srid(int srid) {
		return set_srid(getDefaultGeometryColumnName(), srid, getDefaultGeometryColumnName());
	}
	
	public SpatialDataset srid(String geomCol, String outCol) {
		int srid = m_jarveySchema.getColumn(geomCol).getJarveyDataType().asGeometryType().getSrid();
		return withRegularColumn(outCol, lit(srid));
	}
	public SpatialDataset srid(String outCol) {
		return srid(getDefaultGeometryColumnName(), outCol);
	}

	public SpatialDataset start_point(String outCol) {
		return withGeometryColumn(outCol, ST_StartPoint(getDefaultGeometryColumnExpr()));
	}
	public SpatialDataset start_point() {
		return start_point(getDefaultGeometryColumnName());
	}

	public SpatialDataset to_wkb(String outCol) {
		return withRegularColumn(outCol, ST_AsBinary(getDefaultGeometryColumnExpr()));
	}

	public SpatialDataset to_wkt(String outCol) {
		return withRegularColumn(outCol, ST_AsText(getDefaultGeometryColumnExpr()));
	}

	public SpatialDataset transform(int toSrid, String outCol) {
		GeometryColumnInfo gcInfo = m_jarveySchema.getDefaultGeometryColumnInfo();
		
		if ( gcInfo.srid() != toSrid ) {
			return withColumn(outCol, ST_Transform(col(gcInfo.name()), gcInfo.srid(), toSrid),
								GeometryType.of(toSrid));
		}
		else if ( !getDefaultGeometryColumnName().equals(outCol) ) {
			return withGeometryColumn(outCol, col(gcInfo.name()));
		}
		else {
			return this;
		}
	}
	public SpatialDataset transform(int toSrid) {
		return transform(toSrid, getDefaultGeometryColumnName());
	}

	public SpatialDataset transform_box(String boxCol, int toSrid, String outCol) {
		GeometryColumnInfo gcInfo = getDefaultGeometryColumnInfo();
		
		if ( gcInfo.srid() != toSrid ) {
			return withColumn(outCol, ST_TransformBox(col(boxCol), lit(gcInfo.srid()), lit(toSrid)),
								EnvelopeType.get());
		}
		else if ( !boxCol.equals(outCol) ) {
			return withColumn(outCol, col(boxCol), EnvelopeType.get());
		}
		else {
			return this;
		}
	}
	public SpatialDataset transform_box(String boxCol, int toSrid) {
		return transform_box(boxCol, toSrid, boxCol);
	}
	
	public SpatialDataset to_x(String geomCol, String outCol) {
		return withRegularColumn(outCol, ST_X(col(geomCol)));
	}
	public SpatialDataset to_x(String outCol) {
		return to_x(getDefaultGeometryColumnName(), outCol);
	}
	public SpatialDataset to_y(String geomCol, String outCol) {
		return withRegularColumn(outCol, ST_Y(col(geomCol)));
	}
	public SpatialDataset to_y(String outCol) {
		return to_y(getDefaultGeometryColumnName(), outCol);
	}

	public static final String ENVL4326 = "_ENVL_4326";
	public static final String CLUSTER_ID = "_CLUSTER_ID";
	public static final String CLUSTER_MEMBERS = "_CLUSTER_MEMBERS";
	public static final Long QID_OUTLIER = Long.valueOf(-9);
	public static final String QKEY_OUTLIER = "X";
	public SpatialDataset attachQuadIds(Long[] candidates) {
		SpatialDataset sds = this.box2d(ENVL4326)
								.transform_box(ENVL4326, 4326);
		sds = sds.withColumn(CLUSTER_MEMBERS, JV_AttachQuadMembers(sds.col(ENVL4326), candidates),
								ArrayType.of(JarveyDataTypes.LongType, false));
		sds = sds.withRegularColumn(CLUSTER_ID, element_at(sds.col(CLUSTER_MEMBERS), 1));
		
		return sds;
	}
	
	public SpatialDataset rangeQuery(Long... quadIds) {
		if ( quadIds.length == 0 ) {
			quadIds = new Long[] {QID_OUTLIER};
		}
		if ( quadIds.length == 1 ) {
			return this.filter(col(CLUSTER_ID).equalTo(quadIds[0]));
		}
		else {
			Column clusterSelector = FStream.of(quadIds)
											.map(qk -> col(CLUSTER_ID).equalTo(qk))
											.reduce((c1, c2) -> c1.or(c2));
			SpatialDataset res = this.filter(clusterSelector);
			
			Column pred = max(array_intersect(col(CLUSTER_MEMBERS), lit(quadIds)))
							.equalTo(col(CLUSTER_ID));
			res = res.filter(pred);
			
			return res;
		}
	}

	public SpatialDataset rangeQuery(Envelope range, Long... qids) {
		int srid = getDefaultGeometryColumnInfo().srid();
		Envelope envl4326 = (srid != 4326)
							? CoordinateTransform.getTransformToWgs84("EPSG:" + srid).transform(range)
							: range;
		Long[] overlapQids = LongFStream.of(qids)
										.filter(qid -> qid != QID_OUTLIER)
										.filter(qid -> MapTile.fromQuadId(qid).intersects(envl4326))
										.toArray(Long.class);
		SpatialDataset sds = rangeQuery(overlapQids);
		return sds.filter(ST_BoxIntersects(sds.col(ENVL4326), envl4326));
	}
	
	public static SpatialDataset rangeQuery(JarveySession jarvey, String dsId, Envelope range) {
		JarveySchema jschema = jarvey.loadJarveySchema(dsId);
		
		return jarvey.read().dataset(dsId).rangeQuery(range, jschema.getQuadIds());
	}

	private static final MapFunction<Row, Long> GetClusterId = (row) -> row.getAs(CLUSTER_ID);
	public SpatialDataset spatialJoin(SpatialDataset right, String outputCols) {
		SpatialJoinOptions opts = SpatialJoinOptions.OUTPUT(outputCols);
		CoGroupSpatialBlockJoin joinFunc = new CoGroupSpatialBlockJoin(this, right, opts);

		JarveySchema outputSchema = joinFunc.getOutputJarveySchema();
		KeyValueGroupedDataset<Long,Row> kvLeft = this.groupByKey(GetClusterId, Encoders.LONG());
		KeyValueGroupedDataset<Long,Row> kvRight = right.groupByKey(GetClusterId, Encoders.LONG());
		Dataset<Row> df = kvLeft.cogroup(kvRight, joinFunc, RowEncoder.apply(outputSchema.getSchema()));
		
		return new SpatialDataset(m_jarvey, df, outputSchema);
	}
	
	public SpatialDataset spatialSemiJoin(SpatialDataset right) {
		SpatialJoinOptions opts = SpatialJoinOptions.DEFAULT;
		CoGroupSemiSpatialJoin joinFunc = new CoGroupSemiSpatialJoin(this, right, opts);

		JarveySchema outputSchema = joinFunc.getOutputJarveySchema();
		KeyValueGroupedDataset<Long,Row> kvLeft = this.groupByKey(GetClusterId, Encoders.LONG());
		KeyValueGroupedDataset<Long,Row> kvRight = right.groupByKey(GetClusterId, Encoders.LONG());
		Dataset<Row> df = kvLeft.cogroup(kvRight, joinFunc, RowEncoder.apply(outputSchema.getSchema()));
		
		return new SpatialDataset(m_jarvey, df, outputSchema);
	}
	
	public SpatialDataset differenceJoin(SpatialDataset right) {
		SpatialJoinOptions opts = SpatialJoinOptions.DEFAULT;
		CoGroupDifferenceJoin joinFunc = new CoGroupDifferenceJoin(this, right, opts);

		JarveySchema outputSchema = joinFunc.getOutputJarveySchema();
		KeyValueGroupedDataset<Long,Row> kvLeft = this.groupByKey(GetClusterId, Encoders.LONG());
		KeyValueGroupedDataset<Long,Row> kvRight = right.groupByKey(GetClusterId, Encoders.LONG());
		Dataset<Row> df = kvLeft.cogroup(kvRight, joinFunc, RowEncoder.apply(outputSchema.getSchema()));
		
		return new SpatialDataset(m_jarvey, df, outputSchema);
	}
	
	private void assertCompatibleSrid(String op, String geomCol1, String geomCol2) {
		int srid1 = m_jarveySchema.getColumn(geomCol1).getJarveyDataType().asGeometryType().getSrid();
		int srid2 = m_jarveySchema.getColumn(geomCol2).getJarveyDataType().asGeometryType().getSrid();
		
		if ( srid1 > 0 && srid2 > 0 && srid1 != srid2 ) {
			throw new DatasetOperationException(String.format("incompatible SRID: op=%s(%s(%s), %s(%s))",
															op, geomCol1, srid1, geomCol2, srid2));
		}
	}
	
	private String getDefaultGeometryColumnName() {
		return m_jarveySchema.getDefaultGeometryColumn().getName().get();
	}
	
	private Column getDefaultGeometryColumnExpr() {
		return col(getDefaultGeometryColumnName());
	}
}
