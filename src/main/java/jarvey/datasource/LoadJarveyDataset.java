package jarvey.datasource;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.yaml.snakeyaml.Yaml;

import jarvey.JarveySession;
import jarvey.support.HdfsPath;
import utils.CSV;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJarveyDataset {
	public static final Dataset<Row> locate(JarveySession jarvey, HdfsPath dir) {
		HdfsPath descPath = dir.child("_meta.yaml");
		try ( Reader reader = new InputStreamReader(descPath.open()) ) {
			Yaml yaml = new Yaml();
			Map<String,Object> dsMeta = yaml.load(reader);
			String dsType = (String)dsMeta.get("type");
			
			Dataset<Row> df;
			switch ( dsType ) {
				case "csv":
					@SuppressWarnings("unchecked")
					Map<String,Object> csvDesc = (Map<String,Object>)dsMeta.get("csv");
					if ( csvDesc == null ) {
						throw new IllegalArgumentException("'csv' description is not found");
					}
					
					CsvDatasetLoader loader = new CsvDatasetLoader(jarvey, dir, csvDesc);
					df = loader.loadDataset();
					break;
				default:
					throw new IllegalArgumentException("unsupported dataset type: " + dsType);
			}
			
			String colsExpr = (String)dsMeta.get("columns");
			if ( colsExpr != null ) {
				Column[] cols = CSV.parseCsv(colsExpr)
									.map(String::trim)
									.map(name -> col(name))
									.toArray(Column.class);
				df = df.select(cols);
			}
			
			return df;
		}
		catch ( IOException e ) {
			throw new IllegalArgumentException("cannot find dataset descriptor file: " + descPath);
		}
	}
}
