package jarvey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;

import utils.func.UncheckedPredicate;
import utils.stream.FStream;

import jarvey.datasource.DatasetException;
import jarvey.datasource.JarveyDataFrameReader;
import jarvey.support.HdfsPath;
import jarvey.type.DataUtils;
import jarvey.type.JarveySchema;
import jarvey.type2.temporal.TemporalUDFs;
import jarvey.udf.SpatialUDFs;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveySession {
	public static final String SCHEMA_FILE = "_JARVEY_SCHEMA";
	
	private final SparkSession m_session;
	private final HdfsPath m_dbRoot;
	
	private JarveySession(SparkSession session, HdfsPath dbRootPath) {
		m_session = session;
		
		UDFRegistration registry = m_session.udf();
		SpatialUDFs.registerUdf(registry);
		TemporalUDFs.registerUdf(registry);

		m_dbRoot = dbRootPath;
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
	
	public HdfsPath getHdfsPath(String dsId) {
		return m_dbRoot.child(dsId);
	}
	
	public JarveyDataFrameReader read() {
		return new JarveyDataFrameReader(this);
	}
	
	public List<String> listDatasets() {
		return m_dbRoot.streamChildFiles()
					.filter(UncheckedPredicate.sneakyThrow(HdfsPath::isDirectory))
					.filter(HdfsPath::isRegular)
					.map(HdfsPath::getName)
					.toList();
	}
	
	public long getDatasetSize(String dsId) {
		try {
			HdfsPath path = getHdfsPath(dsId);
			return FStream.from(path.walkRegularFileTree())
							.mapOrThrow(HdfsPath::getLength)
							.mapToLong(DataUtils::asLong)
							.sum();
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to walk directory: cause=" + e);
		}
	}
	
	public JarveySchema loadJarveySchema(String dsId) {
		HdfsPath dsPath = getHdfsPath(dsId);
		if ( !dsPath.exists() ) {
			throw new IllegalArgumentException("SpatialDataset not found: " + dsId);
		}
		
		HdfsPath infoFile = dsPath.child(SCHEMA_FILE);
		if ( infoFile.exists() ) {
			try ( InputStream is = infoFile.open() ) {
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
	
	public static final class Builder {
		SparkSession.Builder m_builder;
		private HdfsPath m_dbRoot;
		
		private Builder() {
			m_builder = SparkSession.builder();
		}
		
		public Builder appName(String name) {
			m_builder = m_builder.appName(name);
			return this;
		}
		
		public Builder master(String master) {
			m_builder = m_builder.master(master);
			return this;
		}
		
		public Builder dbRoot(HdfsPath rootPath) {
			m_dbRoot = rootPath;
			return this;
		}
		
		public JarveySession getOrCreate() {
			SparkSession session = m_builder.getOrCreate();
			return new JarveySession(session, m_dbRoot);
		}
	}
	
	public static final class BuilderOld {
		SparkSession.Builder m_builder;
		private String m_warehouseRoot = "jarvey/warehouse";
		
		private BuilderOld() {
			m_builder = SparkSession.builder();
		}
		
		public BuilderOld appName(String name) {
			m_builder = m_builder.appName(name);
			return this;
		}
		
		public BuilderOld master(String master) {
			m_builder = m_builder.master(master);
			return this;
		}
		
		public BuilderOld warehouseRoot(String path) {
			if ( !path.endsWith("/") ) {
				path = path + "/";
			}
			
			m_warehouseRoot = path;
			return this;
		}
		
		public JarveySession getOrCreate() {
			Configuration conf = getHadoopConfiguration();
			
			HdfsPath warehouseRoot;
			try {
				HdfsPath path = HdfsPath.of(conf, new Path(m_warehouseRoot));
				if ( !path.exists() ) {
					path.mkdir();
				}
				warehouseRoot = path.getAbsolutePath();
			}
			catch ( IOException e ) {
				throw new IllegalArgumentException("invalid warehouse root path: " + m_warehouseRoot);
			}
			
			SparkSession session = m_builder.config("spark.sql.warehouse.dir", warehouseRoot.toString())
											.enableHiveSupport()
											.getOrCreate();
			return new JarveySession(session, warehouseRoot);
		}
	}
	
	private static Configuration getHadoopConfiguration() {
		//
		// Hadoop-related configuration
		//
		Configuration conf = new Configuration();
		String confHome = System.getenv("HADOOP_CONF_DIR");
		if ( confHome == null ) {
			throw new JarveyRuntimeException("Environment variable is not set: 'HADOOP_CONF_DIR'");
		}
		File coreSiteFile = new File(new File(confHome), "core-site.xml");
		try {
			conf.addResource(new FileInputStream(coreSiteFile));
		}
		catch ( FileNotFoundException e ) {
			throw new JarveyRuntimeException("Hadoop configuration file not found: " + coreSiteFile);
		}
		
		return conf;
	}
}
