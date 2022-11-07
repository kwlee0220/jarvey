package jarvey.datasource.shp;

import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.google.common.collect.Sets;

/**
 *
 * @author Kang-Woo Lee
 */
class ShapefileTable implements SupportsRead {
	private final StructType m_schema;
	
	private static final Set<TableCapability> CAPABILITIES = Sets.newHashSet(TableCapability.BATCH_READ);
	
	ShapefileTable(StructType schema, Map<String,String> props) {
		m_schema = schema;
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
		return CAPABILITIES;
	}

	@Override
	public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
		return new ShapefileTableScanBuilder(m_schema, options);
	}
}
