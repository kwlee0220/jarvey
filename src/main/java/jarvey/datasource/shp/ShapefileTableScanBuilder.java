package jarvey.datasource.shp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee
 */
class ShapefileTableScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {
	private final StructType m_shpFileSchema;
	private final CaseInsensitiveStringMap m_opts;
	private StructType m_requiredSchema;
	private int[] m_mapping;
	
	ShapefileTableScanBuilder(StructType schema, CaseInsensitiveStringMap options) {
		m_shpFileSchema = schema;
		m_requiredSchema = schema;
		m_opts = options;
		
		m_mapping = null;
	}

	@Override
	public Scan build() {
		return new ShpScan();
	}

	@Override
	public void pruneColumns(StructType requiredSchema) {
		if ( m_shpFileSchema.equals(requiredSchema) ) {
			return;
		}

		m_requiredSchema = requiredSchema;
		List<String> shpFieldNames = FStream.of(m_shpFileSchema.fields())
										.map(StructField::name)
										.toList();

		StructField[] fields = requiredSchema.fields();
		m_mapping = new int[fields.length];
		for ( int i =0; i < m_mapping.length; ++i ) {
			int idx = shpFieldNames.indexOf(fields[i].name());
			m_mapping[i] = idx;
		}
	}
	
	private class ShpScan implements Scan {
		@Override
		public StructType readSchema() {
			return m_requiredSchema;
		}
		
		@Override
		public Batch toBatch() {
			try {
				return new ShapefileBatch(m_requiredSchema, m_mapping, m_opts);
			}
			catch ( IOException e ) {
				throw new UncheckedIOException(e);
			}
		}
	}
}
