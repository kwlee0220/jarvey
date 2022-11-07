package jarvey.datasource.shp;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import jarvey.type.JarveySchema;

import utils.Utilities;


/**
*
* @author Kang-Woo Lee
*/
class MultiShapefilesReaderFactory implements PartitionReaderFactory {
	private static final long serialVersionUID = 1L;
	
	private final JarveySchema m_jschema;
	private final String m_charset;
	private final int[] m_mapping;
	
	MultiShapefilesReaderFactory(JarveySchema jschema, String charset, int[] mapping) {
		m_jschema = jschema;
		m_charset = charset;
		m_mapping = mapping;
	}

	@Override
	public MultiShapefilesReader createReader(InputPartition partition) {
		Utilities.checkArgument(partition instanceof MultiShapefilesPartition,
								"invalid InputPartition: not ShpInputPartition, but=" + partition.getClass());
		MultiShapefilesPartition part = (MultiShapefilesPartition)partition;
		
		return new MultiShapefilesReader(m_jschema, m_charset, m_mapping, part);
	}
}