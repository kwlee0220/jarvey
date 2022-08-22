package jarvey.datasource;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.support.HdfsPath;
import jarvey.type.JarveySchema;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataFrameWriter {
	private final SpatialDataset m_sds;
	private final DataFrameWriter<Row> m_writer;
	
	public JarveyDataFrameWriter(SpatialDataset sds) {
		this(sds, sds.getDataFrame().write());
	}
	
	public JarveyDataFrameWriter(SpatialDataset sds, DataFrameWriter<Row> writer) {
		m_sds = sds;
		m_writer = writer;
	}
	
	public JarveyDataFrameWriter mode(SaveMode mode) {
		return new JarveyDataFrameWriter(m_sds, m_writer.mode(mode));
	}
	
	public JarveyDataFrameWriter partitionBy(String... colNames) {
		return new JarveyDataFrameWriter(m_sds, m_writer.partitionBy(colNames));
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

	public void dataset(String dsId, Long[] qids) {
		HdfsPath dsPath = m_sds.getJarveySession().getHdfsPath(dsId);
		String fullPath = dsPath.toString();
		m_writer.option("path", fullPath)
				.saveAsTable(dsId);
		
		JarveySchema jschema = m_sds.getJarveySchema();
		if ( qids != null ) {
			jschema = jschema.toBuilder().setQuadIds(qids).build();
		}

		HdfsPath schemaPath = dsPath.child(JarveySession.SCHEMA_FILE);
		try ( OutputStream os = schemaPath.create(true, 64*1024*1024) ) {
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