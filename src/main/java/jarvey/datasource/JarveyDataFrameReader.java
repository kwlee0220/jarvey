package jarvey.datasource;

import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.yaml.snakeyaml.Yaml;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.datasource.shp.ShpTableProvider;
import jarvey.support.HdfsPath;
import jarvey.type.JarveySchema;
import utils.CSV;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyDataFrameReader extends DataFrameReader {
	private final JarveySession m_jarvey;
	
	public JarveyDataFrameReader(JarveySession session) {
		super(session.spark());
		
		m_jarvey = session;
	}
	
	public Dataset<Row> shapefile(String path) {
		return this.format(ShpTableProvider.class.getName()).load(path);
	}

	public SpatialDataset dataset(String dsId) {
		HdfsPath dsPath = m_jarvey.getHdfsPath(dsId);
		
		HdfsPath descPath = dsPath.child("_descriptor.yaml");
		if ( !descPath.exists() ) {
			HdfsPath schemaPath = dsPath.child(JarveySession.SCHEMA_FILE);
			try ( InputStream is = schemaPath.open() ) {
				JarveySchema jschema = JarveySchema.readYaml(is);
				Dataset<Row> df = m_jarvey.spark().table(dsId);
				return new SpatialDataset(m_jarvey, df, jschema);
			}
			catch ( IOException e ) {
				throw new DatasetException("fails to read jarvey schema file: " + schemaPath);
			}
		}
		
		try ( Reader reader = new InputStreamReader(descPath.open()) ) {
			Yaml yaml = new Yaml();
			Map<String,Object> dsDesc = yaml.load(reader);

			SpatialDataset sds;
			String dsType = (String)dsDesc.get("type");
			switch ( dsType ) {
				case "csv":
					@SuppressWarnings("unchecked")
					Map<String,Object> csvDesc = (Map<String,Object>)dsDesc.get("csv");
					if ( csvDesc == null ) {
						throw new DatasetException("'csv' description is not found");
					}
					
					CsvDatasetLoader loader = new CsvDatasetLoader(m_jarvey, dsPath, csvDesc);
					sds = loader.loadDataset();
					break;
				default:
					throw new DatasetException("unsupported dataset type: " + dsType);
			}
			
			String colsExpr = (String)dsDesc.get("columns");
			if ( colsExpr != null ) {
				Column[] cols = CSV.parseCsv(colsExpr)
									.map(String::trim)
									.map(name -> col(name))
									.toArray(Column.class);
				sds = sds.select(cols);
			}
			
			colsExpr = (String)dsDesc.get("drop_columns");
			if ( colsExpr != null ) {
				String[] cols = CSV.parseCsv(colsExpr)
									.map(String::trim)
									.toArray(String.class);
				sds = sds.drop(cols);
			}
			
			return sds;
		}
		catch ( IOException e ) {
			throw new DatasetException("cannot read dataset descriptor file: " + descPath);
		}
	}
	
	private static final String MEMBERSHIP = SpatialDataset.CLUSTER_MEMBERS;
	private static final String CLUSTER_ID = SpatialDataset.CLUSTER_ID;
	private static final String TEMP = "__matches";
	public SpatialDataset clusters(String dsId, Long... quadIds) {
		if ( quadIds.length == 1 ) {
			return dataset(dsId).filter(col(CLUSTER_ID).equalTo(quadIds[0]));
		}
		else {
			Column clusterSelector = FStream.of(quadIds)
											.map(qk -> col(CLUSTER_ID).equalTo(qk))
											.reduce((c1, c2) -> c1.or(c2));
			SpatialDataset sds = dataset(dsId).filter(clusterSelector);
			sds = sds.withRegularColumn(TEMP, array_intersect(lit(quadIds), col(MEMBERSHIP)));
			
			Column isNonDuplicate = element_at(col(TEMP), 1).equalTo(col(CLUSTER_ID));
			sds = sds.filter(isNonDuplicate).drop(TEMP);
			
			return sds;
		}
	}
	
	public JarveyDataFrameReader option(String key, String value) {
		return (JarveyDataFrameReader)super.option(key, value);
	}
}
