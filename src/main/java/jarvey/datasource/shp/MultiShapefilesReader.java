package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.datasource.ChainedPartitionReader;
import jarvey.type.JarveySchema;

/**
*
* @author Kang-Woo Lee
*/
class MultiShapefilesReader extends ChainedPartitionReader<ShapefileReader> {
	static final Logger s_logger = LoggerFactory.getLogger(MultiShapefilesReader.class);
	
	private final JarveySchema m_jschema;
	private final String m_charset;
	private final int[] m_mapping;
	
	private final int m_partIndex;
	private final File[] m_files;
	private int m_fileIndex;
	
	MultiShapefilesReader(JarveySchema jschema, String charset, int[] mapping, MultiShapefilesPartition partition) {
		m_jschema = jschema;
		m_charset = charset;
		m_mapping = mapping;
		
		m_partIndex = partition.getIndex();
		m_files = partition.getFiles();
		m_fileIndex = -1;
		
		setLogger(s_logger);
	}
	
	@Override
	public String toString() {
		return String.format("Partion[%d]: (%d/%d): %s", m_partIndex, m_fileIndex+1, m_files.length,
														Arrays.toString(m_files));
	}
	
	@Override
	protected ShapefileReader getNextPartitionReader() throws IOException {
		if ( ++m_fileIndex < m_files.length ) {
			return new ShapefileReader(m_jschema, m_files[m_fileIndex], m_charset, m_mapping);
		}
		else {
			return null;
		}
	}
	
	protected void onComponentPartitionReaderClosed(ShapefileReader component) {
		if ( getLogger().isInfoEnabled() ) {
			String msg = String.format("shp loaded: Partion[%d]: (%d/%d) %s",
										m_partIndex, m_fileIndex+1, m_files.length, component.getMetricString());
			getLogger().info(msg);
		}
		
	}
}