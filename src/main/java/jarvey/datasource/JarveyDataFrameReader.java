package jarvey.datasource;

import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.array_max;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Maps;

import jarvey.FilePath;
import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.datasource.shp.ShapefileDataSets;
import jarvey.datasource.shp.ShapefileTableProvider;
import jarvey.optor.geom.RangePredicate;
import jarvey.support.MapTile;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;

import utils.CSV;
import utils.Utilities;
import utils.geo.util.CoordinateTransform;
import utils.geo.util.GeometryUtils;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataFrameReader {
	private static final Logger s_logger = LoggerFactory.getLogger(JarveyDataFrameReader.class);
	
	private final JarveySession m_jarvey;
	private DataFrameReader m_reader;
	private Map<String,String> m_options = Maps.newHashMap();
	
	public JarveyDataFrameReader(JarveySession session) {
		m_jarvey = session;
		m_reader = new DataFrameReader(session.spark());
	}
	
	JarveyDataFrameReader schema(StructType schema) {
		m_reader = m_reader.schema(schema);
		return this;
	}
	
	public DataFrameReader getDataFrameReader() {
		return m_reader;
	}
	
	public SpatialDataFrame shapefile(File path) {
		Dataset<Row> df = m_reader.format(ShapefileTableProvider.class.getName())
									.load(path.getAbsolutePath());
		
		int srid = Integer.parseInt(m_options.getOrDefault("srid", "0"));
		GeometryType defGeomType = ShapefileDataSets.loadDefaultGeometryType(path, srid);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", defGeomType);
		
		JarveySchema jschema = JarveySchema.fromStructType(df.schema(), gcInfo);
		
		return m_jarvey.toSpatial(df, jschema);
	}
	
	public SpatialDataFrame csv(String dsId) {
		FilePath dsPath = m_jarvey.getDatasetFilePath(dsId);
		
		Dataset<Row> df = m_reader.csv(dsPath.getAbsolutePath());
		JarveySchema jschema = JarveySchema.fromStructType(df.schema(), null);
		return m_jarvey.toSpatial(df, jschema);
	}
	
	public Dataset<Row> parquet(FilePath path) {
		return m_reader.parquet(path.getAbsolutePath());
	}

	public SpatialDataFrame dataset(String dsId) {
		Utilities.checkNotNullArgument(dsId);
		
		FilePath dsPath = m_jarvey.getDatasetFilePath(dsId);
		if ( !dsPath.exists() ) {
			throw new DatasetException("dataset not found: id=" + dsId);
		}
		
		return dataset(dsPath);
	}

	public SpatialDataFrame dataset(FilePath dsPath) {
		FilePath descPath = dsPath.getChild("_descriptor.yaml");
		if ( !descPath.exists() ) {
			FilePath schemaPath = dsPath.getChild(JarveySession.SCHEMA_FILE);
			JarveySchema jschema = readJarveySchema(schemaPath);
			Dataset<Row> df = m_jarvey.spark().read().parquet(dsPath.getAbsolutePath());
			return m_jarvey.toSpatial(df, jschema);
		}
		
		try ( Reader reader = new InputStreamReader(descPath.read()) ) {
			Yaml yaml = new Yaml();
			Map<String,Object> dsDesc = yaml.load(reader);

			SpatialDataFrame sds;
			String dsType = (String)dsDesc.get("type");
			switch ( dsType ) {
				case "csv":
					@SuppressWarnings("unchecked")
					Map<String,Object> csvDesc = (Map<String,Object>)dsDesc.get("csv");
					if ( csvDesc == null ) {
						throw new DatasetException("'csv' description is not found");
					}
					
					CsvDatasetLoader loader = new CsvDatasetLoader(m_jarvey, m_reader, dsPath, csvDesc);
					sds = loader.load();
					break;
				default:
					throw new DatasetException("unsupported dataset type: " + dsType);
			}
			
			String colsExpr = (String)dsDesc.get("drop_columns");
			if ( colsExpr != null ) {
				String[] cols = CSV.parseCsv(colsExpr)
									.map(String::trim)
									.toArray(String.class);
				sds = sds.drop(cols);
			}
			
			colsExpr = (String)dsDesc.get("columns");
			if ( colsExpr != null ) {
				String[] cols = CSV.parseCsv(colsExpr).map(String::trim).toArray(String.class);
				sds = sds.select(cols);
			}
			
			return sds;
		}
		catch ( IOException e ) {
			throw new DatasetException("cannot read dataset descriptor file: " + descPath);
		}
	}

	public SpatialDataFrame clusters(String dsId, boolean distinct) {
		SpatialDataFrame sdf = loadClusteredDataFrame(dsId);
		
		if ( distinct ) {
			Column partIdExpr = sdf.col(SpatialDataFrame.COLUMN_PARTITION_QID);
			Column quidIdsColExpr = sdf.col(SpatialDataFrame.COLUMN_QUAD_IDS);
			sdf = sdf.filter(element_at(quidIdsColExpr, 1).equalTo(partIdExpr));
		}
		return sdf;
	}

	public SpatialDataFrame clusters(String dsId, long[] quadIds) {
		SpatialDataFrame sdf = loadClusteredDataFrame(dsId);

		return queryByQuadSpaces(sdf, quadIds);
	}

	/**
	 * 주어진 식별자의 cluster 파일에 저장된 레코드 중에서 주어진 영역과 겹치는 것으로
	 * 구성된 {@link SpatialDataFrame}를 반환한다.
	 * 
	 * @param cluster 	식별자
	 * @param range		검색 영역.
	 * 					영역의 공간 좌표의 SRID는 cluster에 저장된 레코드들의
	 * 					default 공간 객체의 SRID와 동일해야 한다.
	 * @return	주어진 영역과 겹치는 레코드들로 구성된 {@link SpatialDataFrame}.
	 */
	public SpatialDataFrame clusters(String dsId, Envelope range) {
		Utilities.checkNotNullArgument(dsId, "cluster dataset id");
		Utilities.checkNotNullArgument(range, "range");
		
		long[] keyQids = queryQuadIds(dsId, range);
		if ( s_logger.isDebugEnabled() ) {
			List<String> qkeyList = FStream.of(keyQids).map(MapTile::toQuadKey).toList();
			s_logger.debug("Load cluster: id={}, quad-spaces={}", dsId, qkeyList);
		}
		
		SpatialDataFrame sdf = loadClusteredDataFrame(dsId);
		
		// 전체 dataframe에서 주어진 quadspace id들에 해당하는 cluster만 로드한다.
		// (Spark SQL의 최적화 모듈에 의존함)
		sdf = queryByQuadSpaces(sdf, keyQids);
		
		// 주어진 영역과 겹치는 geometry를 갖는 레코드를 검색한다.
		sdf = sdf.filter(RangePredicate.intersects(range));
		
		return sdf;
	}
	public SpatialDataFrame clustersJustForTest(String dsId, Envelope range) {
		long[] keyQids = queryQuadIds(dsId, range);
		if ( s_logger.isInfoEnabled() ) {
			List<String> qkeyList = FStream.of(keyQids).map(MapTile::toQuadKey).toList();
			s_logger.info("Load cluster: id={}, quad-spaces={}", dsId, qkeyList);
		}
		
		SpatialDataFrame sdf = loadClusteredDataFrame(dsId);
		
		// 전체 dataframe에서 주어진 quadspace id들에 해당하는 cluster만 로드한다.
		// (Spark SQL의 최적화 모듈에 의존함)
		sdf = queryByQuadSpaces(sdf, keyQids);
		
		return sdf;
	}
	
	public static JarveySchema readJarveySchema(FilePath schemaPath) {
		try ( InputStream is = schemaPath.read() ) {
			return JarveySchema.readYaml(is);
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to read jarvey schema file: " + schemaPath, e);
		}
	}
	
	public JarveySchema readClusterJarveySchema(String dsId) {
		FilePath clusterPath = m_jarvey.getClusterFilePath(dsId);
		FilePath schemaPath = clusterPath.getChild(JarveySession.SCHEMA_FILE);
		return readJarveySchema(schemaPath);
	}
	
	public long[] quadSpaceIds(String dsId) {
		FilePath path = m_jarvey.getQuadSetFilePath(dsId);
		
		Dataset<Row> df = m_reader.csv(path.getAbsolutePath());
		return FStream.from(df.collectAsList()).mapToLong(r -> Long.parseLong(r.getString(0))).toArray();
	}
	
	public JarveyDataFrameReader option(String key, String value) {
		m_options.put(key, value);
		m_reader.option(key, value);
		return this;
	}
	
	private SpatialDataFrame loadClusteredDataFrame(String dsId) {
		FilePath clusterPath = m_jarvey.getClusterFilePath(dsId);
		FilePath schemaPath = clusterPath.getChild(JarveySession.SCHEMA_FILE);
		JarveySchema jschema = readJarveySchema(schemaPath);
		
		Dataset<Row> df = m_jarvey.spark().read().parquet(clusterPath.getAbsolutePath());
		
		String partCol = SpatialDataFrame.COLUMN_PARTITION_QID;
		df = df.withColumn(partCol, df.col(partCol).cast("long"));
		
//		// 'partitionBy()'를 적용한 후 저장된 parquet 파일은 원래 column 순서가 보장되지 않기 때문에
//		// 읽은 dataset의 column 순서를 스키마에 맞도록 조정한다.
//		Column[] cols = FStream.from(jschema.getColumnAll())
//								.map(jcol -> df.col(jcol.getName().get()))
//								.toArray(Column.class);
		return m_jarvey.toSpatial(df, jschema);
	}

	private SpatialDataFrame queryByQuadSpaces(SpatialDataFrame sdf, long[] quadIds) {
		Column partIdCol = sdf.col(SpatialDataFrame.COLUMN_PARTITION_QID);
		Column quidIdsCol = sdf.col(SpatialDataFrame.COLUMN_QUAD_IDS);

		if ( quadIds.length == 0 ) {
			sdf = sdf.filter(element_at(quidIdsCol, 0).equalTo(partIdCol));
		}
		else if ( quadIds.length == 1 ) {
			sdf = sdf.filter(partIdCol.equalTo(lit(quadIds[0])));
		}
		else {
			// 주어진 quad-id와 관련된 paritition file만 적재시킨다.
			Column partSelection = FStream.of(quadIds)
											.map(qid -> partIdCol.equalTo(lit(qid)))
											.reduce((c1, c2) -> c1.or(c2));
			sdf = sdf.filter(partSelection);
			
			// 각 레코드의 소속 quad-id와 질의 quid-id들의 교집합을 구하기 위한 column-expression 생성
			Column qidOverlaps = array_intersect(lit(quadIds), quidIdsCol);
			
			// primary quad-id를 선택하는 column-expression 생성
			Column primaryQuidExpr = array_max(qidOverlaps);
			
			// 선택된 quad-id와 각 레코드의 primary quad-id가 같은 경우만 레코드를
			// 선택하는 column-expression 생성
			Column isSamePrimaryExpr = primaryQuidExpr.equalTo(partIdCol);
			
			sdf = sdf.filter(isSamePrimaryExpr);
		}
		
		return sdf;
	}

	private long[] queryQuadIds(String dsId, Envelope range) {
		FilePath clusterPath = m_jarvey.getClusterFilePath(dsId);
		FilePath schemaPath = clusterPath.getChild(JarveySession.SCHEMA_FILE);
		JarveySchema jschema = readJarveySchema(schemaPath);
		
		Envelope range4326 = (jschema.getSrid() != 4326)
							? CoordinateTransform.transformToWgs84(range, "EPSG:" + jschema.getSrid())
							: range;
		
		// 주어진 range과 겹치는 quadspace의 id를 찾는다.
		long[] keyQids = FStream.of(jschema.getQuadIds())
								.map(MapTile::fromQuadId)
								.filter(tile -> tile.getBounds().intersects(range4326))
								.mapToLong(MapTile::getQuadId)
								.toArray();
		
		// 만일 range 영역 중 quadspace와 겹치지 않는 영역이 있다면 outlier_qid도 포함시킨다.
		Geometry extent = FStream.of(jschema.getQuadIds())
								.filter(qid -> qid != MapTile.OUTLIER_QID)
								.map(MapTile::fromQuadId)
								.map(tile -> (Geometry)tile.getBoundsPolygon())
								.reduce((p1,p2) -> p1.union(p2));
		Geometry diff = GeometryUtils.toPolygon(range4326).difference(extent);
		if ( diff.getArea() > 0 ) {
			keyQids = Utilities.concat(keyQids, -9L);
		}
		
		return keyQids;
	}
}
