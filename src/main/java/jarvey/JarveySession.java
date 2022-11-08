package jarvey;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Stack;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.StructField;

import com.google.common.primitives.Longs;

import jarvey.cluster.QuadSpacePartitioner;
import jarvey.datasource.DatasetException;
import jarvey.datasource.JarveyDataFrameReader;
import jarvey.optor.RecordSerDe;
import jarvey.support.RecordLite;
import jarvey.type.DataUtils;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveySchema;
import jarvey.type.temporal.TemporalUDFs;
import jarvey.udf.SpatialUDFs;

import utils.Utilities;
import utils.func.Unchecked;
import utils.io.FilePath;
import utils.io.LfsPath;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySession implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String SCHEMA_FILE = "_JARVEY_SCHEMA";
	private static final String ROOT_DATASETS = "datasets";
	private static final String ROOT_CLUSTERS = "clusters";
	private static final String ROOT_QUAD_SETS = "quad_sets";
	
	private final SparkSession m_session;
	private final FilePath m_root;
	private final FilePath m_datasetsRoot;
	private final FilePath m_clustersRoot;
	private final FilePath m_quadSetsRoot;
	
	private JarveySession(SparkSession session, FilePath rootPath) {
		Utilities.checkNotNullArgument(session, "SparkSession");
		Utilities.checkNotNullArgument(rootPath, "dbRootPath");
		
		m_session = session;
		
		UDFRegistration registry = m_session.udf();
		SpatialUDFs.registerUdf(registry);
		TemporalUDFs.registerUdf(registry);

		m_root = rootPath;
		m_datasetsRoot = rootPath.getChild(ROOT_DATASETS);
		m_clustersRoot = rootPath.getChild(ROOT_CLUSTERS);
		m_quadSetsRoot = rootPath.getChild(ROOT_QUAD_SETS);
	}
	
	public static JarveySession of(SparkSession spark) {
		return new JarveySession(spark, null);
	}
	
	public static JarveySession.Builder builder() {
		return new Builder();
	}
	
	public SparkSession spark() {
		return m_session;
	}
	
	public JavaSparkContext javaSc() {
		return new JavaSparkContext(spark().sparkContext());
	}
	
	public void stop() {
		m_session.stop();
	}

	public SpatialDataFrame toSpatial(JavaRDD<Row> rows, JarveySchema jschema) {
		Dataset<Row> df = m_session.createDataFrame(rows, jschema.getSchema());
		return toSpatial(df, jschema);
	}
	public SpatialDataFrame toSpatial(Dataset<Row> df, JarveySchema jschema) {
		for ( int i =0; i < jschema.getColumnCount(); ++i ) {
			JarveyColumn jcol = jschema.getColumn(i);
			StructField field = df.schema().fields()[i];
			
			if ( !jcol.getName().equals(field.name()) ) {
				String msg = String.format("incompatible schema (field_name): %s <-> %s", jcol.getName(), field.name());
				throw new IllegalArgumentException(msg);
			}
			
			if ( !jcol.getJarveyDataType().getSparkType().equals(field.dataType()) ) {
				String msg = String.format("incompatible schema (field_type): %s <-> %s (%s)",
											jcol.getJarveyDataType().getSparkType(), field.dataType(),
											field.name());
				throw new IllegalArgumentException(msg);
			}
		}
		
		return new SpatialDataFrame(this, jschema, df);
	}
	public SpatialDataFrame toSpatial(Dataset<Row> df, @Nullable GeometryColumnInfo defaultGcInfo) {
		JarveySchema jschema = JarveySchema.fromStructType(df.schema(), defaultGcInfo);
		return toSpatial(df, jschema);
	}
	public SpatialDataFrame toSpatial(Dataset<Row> df) {
		JarveySchema jschema = JarveySchema.fromStructType(df.schema(), null);
		return toSpatial(df, jschema);
	}
	
	public FilePath getRootPath() {
		return m_root;
	}
	public FilePath getDatasetRootPath() {
		return m_datasetsRoot;
	}
	
	public FilePath getDatasetFilePath(String dsId) {
		if ( dsId.startsWith("/") ) {
			dsId = dsId.substring(1);
		}
		return m_datasetsRoot.getChild(dsId);
	}
	
	public String toDatasetId(FilePath path) {
		int prefixLen = m_datasetsRoot.getAbsolutePath().length();
		return path.getAbsolutePath().substring(prefixLen);
	}
	
	public FilePath getClusterFilePath(String dsId) {
		if ( dsId.startsWith("/") ) {
			dsId = dsId.substring(1);
		}
		return m_clustersRoot.getChild(dsId);
	}
	public FilePath getQuadSetFilePath(String dsId) {
		if ( dsId.startsWith("/") ) {
			dsId = dsId.substring(1);
		}
		return m_quadSetsRoot.getChild(dsId);
	}
	
	public JarveyDataFrameReader read() {
		return new JarveyDataFrameReader(this);
	}
	
	public boolean existsSpatialDataFrame(String sdfId) {
		return getDatasetFilePath(sdfId).exists();
	}
	
	public void deleteSpatialDataFrame(String dsId) {
		Unchecked.runOrIgnore(() -> getQuadSetFilePath(dsId).deleteUpward());
		Unchecked.runOrIgnore(() -> getClusterFilePath(dsId).deleteUpward());
		Unchecked.runOrIgnore(() -> getDatasetFilePath(dsId).deleteUpward());
	}
	
	public boolean isDatasetFilePath(FilePath path) {
		if ( !path.isDirectory() || !path.isRegular() ) {
			return false;
		}
		
		return path.getChild(JarveySession.SCHEMA_FILE).exists()
					|| path.getChild("_descriptor.yaml").exists();
	}
	
	public List<String> listDatasets() {
		try {
			return m_datasetsRoot.walkTree(false)
								.filter(this::isDatasetFilePath)
								.map(this::toDatasetId)
								.toList();
		}
		catch ( IOException e ) {
			throw new DatasetException(e);
		}
	}
	
	public long calcDatasetLength(String dsId) {
		try {
			FilePath path = getDatasetFilePath(dsId);
			return FStream.from(path.walkRegularFileTree())
							.mapOrThrow(FilePath::getLength)
							.mapToLong(DataUtils::asLong)
							.sum();
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to walk directory: cause=" + e);
		}
	}
	
	public JarveySchema loadJarveySchema(String dsId) {
		FilePath dsPath = getDatasetFilePath(dsId);
		if ( !dsPath.exists() ) {
			throw new IllegalArgumentException("SpatialDataset not found: " + dsId);
		}
		
		FilePath infoFile = dsPath.getChild(SCHEMA_FILE);
		if ( infoFile.exists() ) {
			try ( InputStream is = infoFile.read() ) {
				return JarveySchema.readYaml(is);
			}
			catch ( IOException e ) {
				throw new DatasetException("fails to read file: " + infoFile);
			}
		}
		else {
			return null;
		}
	}
	
	public SpatialDataFrame parallelize(List<RecordLite> recList, JarveySchema jschema) {
		RecordSerDe serde = RecordSerDe.of(jschema);
		List<Row> rowList = FStream.from(recList).map(serde::serialize).toList();
		Dataset<Row> df = spark().createDataFrame(rowList, jschema.getSchema())
								.coalesce(1);
		
		return new SpatialDataFrame(this, jschema, df);
	}
	
	public <T> Dataset<Row> parallelize(List<T> row, Class<T> beanCls) {
		JavaRDD<T> jrdd = javaSc().parallelize(row);
		return spark().createDataFrame(jrdd, beanCls);
	}

	public QuadSpacePartitioner getQuadSpacePartitioner(String dsId) {
		long[] quadIds = read().quadSpaceIds(dsId);
		return getQuadSpacePartitioner(quadIds);
	}
	public QuadSpacePartitioner getQuadSpacePartitioner(long[] quadIds) {
		return QuadSpacePartitioner.from(Longs.asList(quadIds));
	}
	
	private Stack<Integer> m_shufflePartitionCounts = new Stack<>();
	public int setShufflePartitionCount(int count) {
		int oldCnt = Integer.parseInt(m_session.conf().get("spark.sql.shuffle.partitions"));
		m_shufflePartitionCounts.push(oldCnt);
		m_session.conf().set("spark.sql.shuffle.partitions", count);
		
		return oldCnt;
	}
	public int unsetShufflePartitionCount() {
		if ( m_shufflePartitionCounts.size() > 0 ) {
			int cnt = m_shufflePartitionCounts.pop();
			m_session.conf().set("spark.sql.shuffle.partitions", cnt);
			
			return cnt;
		}
		else {
			return Integer.parseInt(m_session.conf().get("spark.sql.shuffle.partitions"));
		}
	}
	
	public static final class Builder {
		SparkSession.Builder m_builder;
		private FilePath m_root;
		
		private Builder() {
			m_builder = SparkSession.builder()
									.config("spark.driver.maxResultSize", "3g")
									.appName("jarvey_app");
//									.master("local[7]");
		}
		
		public Builder appName(String name) {
			m_builder = m_builder.appName(name);
			return this;
		}
		
		public Builder master(String master) {
			m_builder = m_builder.master(master);
			return this;
		}
		public Builder executorCount(int count) {
			m_builder = m_builder.config("spark.executor.instances", count);
			return this;
		}
		
		public Builder root(File root) {
			m_root = LfsPath.of(root);
			return this;
		}
		
		public Builder hadoopDatasetRoot(Path hadoopConfigPath, String rootPath) {
			try {
				Configuration conf = new Configuration();
				conf.addResource(hadoopConfigPath);
				FileSystem fs = FileSystem.get(conf);
				m_root = HdfsPath.of(fs, rootPath);
				
				return this;
			}
			catch ( IOException e ) {
				throw new UncheckedIOException(e);
			}
		}
		
		public JarveySession getOrCreate() {
			if ( m_root == null ) {
				hadoopDatasetRoot(new Path("jarvey-hadoop.xml"), "jarvey");
			}
			
			SparkSession session = m_builder.getOrCreate();
			return new JarveySession(session, m_root);
		}
	}
}
