package jarvey.datasource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import jarvey.FilePath;
import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.type.JarveySchema;

import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataFrameWriter {
	private final SpatialDataFrame m_sds;
	private final DataFrameWriter<Row> m_writer;
	
	public JarveyDataFrameWriter(SpatialDataFrame sds) {
		this(sds, sds.toDataFrame(false).write());
	}
	
	public JarveyDataFrameWriter(SpatialDataFrame sds, DataFrameWriter<Row> writer) {
		m_sds = sds;
		m_writer = writer;
	}
	
	public JarveyDataFrameWriter mode(SaveMode mode) {
		return new JarveyDataFrameWriter(m_sds, m_writer.mode(mode));
	}
	
	public JarveyDataFrameWriter force(boolean flag) {
		return mode(flag ? SaveMode.Overwrite : SaveMode.ErrorIfExists);
	}
	
	public JarveyDataFrameWriter partitionBy(String... colNames) {
		JarveySchema schema = m_sds.getJarveySchema();
		
		JarveySchema writeSchema = JarveySchema.concat(schema.complement(Arrays.asList(colNames)),
														schema.select(colNames));
		String[] writeColNames = FStream.from(writeSchema.getColumnAll())
										.map(jc -> jc.getName().get())
										.toArray(String.class);
		SpatialDataFrame sdf = m_sds.select(writeColNames);
		DataFrameWriter<Row> writer = sdf.toDataFrame().write().partitionBy(colNames);
		return new JarveyDataFrameWriter(sdf, writer);
	}
	
	public JarveyDataFrameWriter bucketBy(int nbuckets, String colName, String...colNames) {
		return new JarveyDataFrameWriter(m_sds, m_writer.bucketBy(nbuckets, colName, colNames));
	}
	
	public JarveyDataFrameWriter sortBy(String colName, String...colNames) {
		return new JarveyDataFrameWriter(m_sds, m_writer.sortBy(colName, colNames));
	}

	public void dataset(String dsId) {
		this.dataset(dsId, null);
	}

	public void dataset(String dsId, long[] qids) {
		writeTo(m_sds.getJarveySession().getDatasetFilePath(dsId), qids);
	}

	public void cluster(String dsId, long[] qids) {
		FilePath clusterPath = m_sds.getJarveySession().getClusterFilePath(dsId);
		writeTo(clusterPath, qids);
	}
	
	private void writeTo(FilePath path, long[] qids) {
		m_writer.parquet(path.getAbsolutePath());
		
		JarveySchema jschema = m_sds.getJarveySchema();
		if ( qids != null ) {
			jschema = jschema.toBuilder().setQuadIds(qids).build();
		}

		FilePath schemaPath = path.getChild(JarveySession.SCHEMA_FILE);
		try ( OutputStream os = schemaPath.create(true) ) {
			jschema.writeAsYaml(os);
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to write Jarvey Schema file: " + schemaPath + ", cause=" + e);
		}
	}
	
//	private void saveSpatialDatasetInfo(HdfsPath dsPath, Long[] qids) {
//		StructType schema = m_sds.getDataFrame().schema();
//		DatasetType dsType = qids != null ? DatasetType.SPATIAL_CLUSTER : DatasetType.HEAP;
//		GeometryColumnInfo gcInfo = m_sds.getDefaultGeometryColumnInfo();
//		SpatialDatasetInfo sdInfo = new SpatialDatasetInfo(dsType, gcInfo, schema, qids);
//		
//		try ( PrintWriter pw = new PrintWriter(dsPath.child(INFO_FILE).create(true, 64*1024*1024)) ) {
//			DumperOptions opts = new DumperOptions();
//			opts.setIndent(2);
//			opts.setPrettyFlow(true);
//			opts.setDefaultFlowStyle(FlowStyle.BLOCK);
//			
//			Yaml yaml = new Yaml(opts);
//			yaml.dump(TypeUtils.toYaml(sdInfo), pw);
//		}
//	}
}