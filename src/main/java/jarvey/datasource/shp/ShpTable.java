/**
 * 
 */
package jarvey.datasource.shp;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import jarvey.type.GeometryType;

/**
 *
 * @author Kang-Woo Lee
 */
public class ShpTable implements SupportsRead {
	private final StructType m_schema;
	private final GeometryType m_defGeomType;
	private final Map<String,String> m_props;
	private Set<TableCapability> m_capabilities = null;
	
	public ShpTable(StructType schema, GeometryType geomType, Map<String,String> props) {
		m_schema = schema;
		m_defGeomType = geomType;
		m_props = props;
	}

	@Override
	public String name() {
		return "ShapefileTable";
	}

	@Override
	public StructType schema() {
		return m_schema;
	}

	@Override
	public Set<TableCapability> capabilities() {
		if ( m_capabilities == null ) {
			m_capabilities = Collections.singleton(TableCapability.BATCH_READ);
		}
		
		return m_capabilities;
	}

	@Override
	public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
		return new ShpScanBuilder(m_schema, m_defGeomType, m_props, options);
	}

}
