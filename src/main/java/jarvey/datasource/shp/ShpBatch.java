/**
 * 
 */
package jarvey.datasource.shp;

import java.io.File;
import java.util.Map;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import jarvey.type.GeometryType;
import utils.func.FOption;


/**
 *
 * @author Kang-Woo Lee
 */
public class ShpBatch implements Batch {
	private final StructType m_schema;
	private final GeometryType m_geomType;
	private final String m_path;
	private final String m_charset;
	
	public ShpBatch(StructType schema, GeometryType geomType, Map<String,String> props,
					CaseInsensitiveStringMap options) {
		m_schema = schema;
		m_geomType = geomType;
		
		m_path = options.get("path");
		m_charset = FOption.ofNullable(options.get("charset")).getOrElse("UTF-8");
	}

	@Override
	public InputPartition[] planInputPartitions() {
		return new InputPartition[]{new ShpInputPartition()};
	}

	@Override
	public PartitionReaderFactory createReaderFactory() {
		return new ShpPartitionReaderFactory(m_schema, m_geomType, m_path, m_charset);
	}
	
	@Override
	public String toString() {
		return String.format("ShapefileBatch: path=%s, charset=%s", m_path, m_charset);
	}
	
	private static class ShpPartitionReaderFactory implements PartitionReaderFactory {
		private static final long serialVersionUID = 1L;
		
		private final StructType m_schema;
		private final GeometryType m_geomType;
		private final String m_path;
		private final String m_charset;
		
		public ShpPartitionReaderFactory(StructType schema, GeometryType geomType, String path, String charset) {
			m_schema = schema;
			m_geomType = geomType;
			m_path = path;
			m_charset = charset;
		}

		@Override
		public PartitionReader<InternalRow> createReader(InputPartition partition) {
			return new ShpPartitionReader(m_schema, m_geomType, new File(m_path), m_charset);
		}
	}
}
