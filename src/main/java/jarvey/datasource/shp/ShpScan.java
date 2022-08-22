package jarvey.datasource.shp;

import java.util.Map;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import jarvey.type.GeometryType;


/**
 *
 * @author Kang-Woo Lee
 */
public class ShpScan implements Scan {
	private final StructType m_schema;
	private final GeometryType m_geomType;
	private final Map<String,String> m_props;
	private final CaseInsensitiveStringMap m_opts;
	
	public ShpScan(StructType schema, GeometryType geomType, Map<String,String> props,
					CaseInsensitiveStringMap options) {
		m_schema = schema;
		m_geomType = geomType;
		m_props = props;
		m_opts = options;
	}

	@Override
	public StructType readSchema() {
		return m_schema;
	}
	
	@Override
	public Batch toBatch() {
		return new ShpBatch(m_schema, m_geomType, m_props, m_opts);
	}
}
