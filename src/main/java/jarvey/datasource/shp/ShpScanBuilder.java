/**
 * 
 */
package jarvey.datasource.shp;

import java.util.Map;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.type.GeometryType;


/**
 *
 * @author Kang-Woo Lee
 */
public class ShpScanBuilder implements ScanBuilder {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(ShpScanBuilder.class);
	
	private final StructType m_schema;
	private final GeometryType m_geomType;
	private final Map<String,String> m_props;
	private final CaseInsensitiveStringMap m_opts;
	
	public ShpScanBuilder(StructType schema, GeometryType geomType, Map<String,String> props,
							CaseInsensitiveStringMap options) {
		m_schema = schema;
		m_geomType = geomType;
		m_props = props;
		m_opts = options;
	}

	@Override
	public Scan build() {
		return new ShpScan(m_schema, m_geomType, m_props, m_opts);
	}
}
