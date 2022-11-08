package jarvey.datasource;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.geotools.geometry.jts.Geometries;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.type.GeometryColumnInfo;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.io.FilePath;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvDatasetLoader {
	private final JarveySession m_jarvey;
	private DataFrameReader m_reader;
	private final FilePath m_root;
	private final Map<String,Object> m_desciption;

	CsvDatasetLoader(JarveySession jarvey, DataFrameReader reader, FilePath root, Map<String,Object> desc) {
		m_jarvey = jarvey;
		m_reader = reader;
		m_root = root;
		m_desciption = desc;
	}
	
	public SpatialDataFrame load() {
		m_reader = applyOptions(m_reader);

		@SuppressWarnings("unchecked")
		Map<String,Object> schemaYaml = (Map<String,Object>)m_desciption.get("schema");
		if ( schemaYaml != null ) {
			StructType schema = parseSchema(schemaYaml);
			m_reader = m_reader.schema(schema);
		}
		
		Dataset<Row> df;
		Object files = m_desciption.get("files");
		if ( files == null ) {
			df = m_reader.csv(m_root.getAbsolutePath());
		}
		else if ( files instanceof List ) {
			String prefix = m_root.getAbsolutePath();
			@SuppressWarnings("unchecked")
			String filesList = FStream.from((List<String>)files)
									.map(file -> prefix + "/" + file)
									.join(',');
			df = m_reader.csv(filesList);
		}
		else if ( files instanceof String ) {
			df = m_reader.csv(m_root.toString() + "/" + files);
		}
		else {
			throw new DatasetException("supported files: " + files);
		}
		
		df = updateColumnTypes(df);
		SpatialDataFrame sds = geometrize(df);
		
		return sds;
	}
	
	private SpatialDataFrame geometrize(Dataset<Row> df) {
		@SuppressWarnings("unchecked")
		Map<String,Object> ptYaml = (Map<String,Object>)m_desciption.get("point");
		if ( ptYaml != null ) {
			String geomCol = (String)ptYaml.getOrDefault("name", "the_geom");
			int srid = (int)ptYaml.getOrDefault("srid", 0);
			String expr = (String)ptYaml.get("expr");
			if ( expr == null ) {
				throw new DatasetException("'expr' is not present in 'point' description");
			}
			df = df.withColumn(geomCol, expr(expr));

			GeometryColumnInfo gcInfo = new GeometryColumnInfo(geomCol, GeometryType.of(Geometries.POINT, srid));
			JarveySchema jschema = JarveySchema.fromStructType(df.schema(), gcInfo);
			return m_jarvey.toSpatial(df, jschema);
		}
		else {
//			throw new DatasetException("CSV dataset does not 'point' description");
			JarveySchema jschema = JarveySchema.fromStructType(df.schema(), null);
			return m_jarvey.toSpatial(df, jschema);
		}
	}
	
	private StructType parseSchema(Map<String,Object> yaml) {
		StructType schema = new StructType();
		for ( Map.Entry<String,Object> ent: yaml.entrySet() ) {
			String name = ent.getKey();
			String typeExpr = ent.getValue().toString();
			JarveyDataType dtype = JarveyDataTypes.fromString(typeExpr);
			
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
