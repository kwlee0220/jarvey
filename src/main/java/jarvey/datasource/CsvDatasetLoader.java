/**
 * 
 */
package jarvey.datasource;

import static jarvey.jarvey_functions.spatial;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.support.HdfsPath;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.JarveyDataType;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvDatasetLoader {
	private final JarveySession m_jarvey;
	private final HdfsPath m_root;
	private final Map<String,Object> m_desciption;

	CsvDatasetLoader(JarveySession jarvey, HdfsPath root, Map<String,Object> desc) {
		m_jarvey = jarvey;
		m_root = root;
		m_desciption = desc;
	}
	
	public SpatialDataset loadDataset() {
		DataFrameReader reader = m_jarvey.read();
		reader = applyOptions(reader);

		@SuppressWarnings("unchecked")
		Map<String,Object> schemaYaml = (Map<String,Object>)m_desciption.get("schema");
		if ( schemaYaml != null ) {
			StructType schema = parseSchema(schemaYaml);
			reader = reader.schema(schema);
		}
		
		Dataset<Row> df;
		Object files = m_desciption.get("files");
		if ( files == null ) {
			df = reader.csv(m_root.toString());
		}
		else if ( files instanceof List ) {
			String prefix = m_root.toString();
			@SuppressWarnings("unchecked")
			String filesList = FStream.from((List<String>)files)
									.map(file -> prefix + "/" + file)
									.join(',');
			df = reader.csv(filesList);
		}
		else if ( files instanceof String ) {
			df = reader.csv(m_root.toString() + "/" + files);
		}
		else {
			throw new DatasetException("supported files: " + files);
		}
		
		df = updateColumnTypes(df);
		SpatialDataset sds = geometrize(df);
		
		return sds;
	}
	
	private SpatialDataset geometrize(Dataset<Row> df) {
		@SuppressWarnings("unchecked")
		Map<String,Object> ptYaml = (Map<String,Object>)m_desciption.get("point");
		if ( ptYaml != null ) {
			String geomCol = (String)ptYaml.getOrDefault("name", "the_geom");
			int srid = (int)ptYaml.getOrDefault("srid", 0);
			String expr = (String)ptYaml.get("expr");
			if ( expr == null ) {
				throw new DatasetException("'expr' is not present in 'point' description");
			}

			GeometryColumnInfo gcInfo = new GeometryColumnInfo(geomCol, srid);
			df = df.withColumn(geomCol, expr(expr));
			return spatial(m_jarvey, df, gcInfo).set_srid(srid);
		}
		else {
			throw new DatasetException("CSV dataset does not 'point' description");
		}
	}
	
	private StructType parseSchema(Map<String,Object> yaml) {
		StructType schema = new StructType();
		for ( Map.Entry<String,Object> ent: yaml.entrySet() ) {
			String name = ent.getKey();
			String typeExpr = ent.getValue().toString();
			JarveyDataType dtype = JarveyDataType.fromString(typeExpr);
			
			schema = schema.add(DataTypes.createStructField(name, dtype.getSparkType(), true));
		}
		
		return schema;
	}
	
	private DataFrameReader applyOptions(DataFrameReader reader) {
		@SuppressWarnings("unchecked")
		Map<String,Object> optsYaml = (Map<String,Object>)m_desciption.get("options");
		if ( optsYaml != null ) {
			for ( Map.Entry<String,Object> ent: optsYaml.entrySet() ) {
				String optKey = ent.getKey();
				String optValue = ent.getValue().toString();
				reader = reader.option(optKey, optValue);
			}
		}
		
		return reader;
	}
	
	private Dataset<Row> updateColumnTypes(Dataset<Row> df) {
		@SuppressWarnings("unchecked")
		Map<String,Object> yaml = (Map<String,Object>)m_desciption.get("update_column_types");
		if ( yaml != null ) {
			for ( Map.Entry<String,Object> ent: yaml.entrySet() ) {
				String fieldName = ent.getKey();
				String fieldTypeName = ent.getValue().toString().toLowerCase();
				df = df.withColumn(fieldName, col(fieldName).cast(fieldTypeName));
			}
		}
		
		return df;
	}
}
