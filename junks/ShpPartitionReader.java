package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.func.UncheckedSupplier;
import utils.geo.Shapefile;
import utils.stream.FStream;

import jarvey.datasource.MultiSourcePartitionReader;
import jarvey.type.GeometryType;
import jarvey.type.JarveySchema;
import jarvey.type.JarveySchemaBuilder;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ShpPartitionReader extends MultiSourcePartitionReader<File> {
	private static final Logger s_logger = LoggerFactory.getLogger(ShpPartitionReader.class);
	
	private final JarveySchema m_jschema;
	private final File m_start;
	private final String m_charset;
	
	ShpPartitionReader(StructType schema, GeometryType geomType, File start, String charset) {
		// collect shp files on the time when 'next()' is called for the first time.
		super(FStream.lazy(UncheckedSupplier.ignore(() -> FStream.from(collectShpFiles(start)))));
		
		JarveySchemaBuilder builder = JarveySchema.builder();
		StructField[] fields = schema.fields();
		for ( int i =0; i < fields.length; ++i ) {
			StructField field = fields[i];
			if ( i == 0 ) {
				builder = builder.addJarveyColumn(field.name(), geomType);
			}
			else {
				builder = builder.addRegularColumn(field.name(), field.dataType());
			}
		}
		
		m_jschema = builder.build();
		m_start = start;
		m_charset = charset;
		
		setLogger(s_logger);
	}

	@Override
	protected PartitionReader<InternalRow> getPartitionReader(File shpFile) throws IOException {
		return new ShapefileReader(m_jschema, shpFile, m_charset);
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s, charset=%s", getClass().getSimpleName(), m_start, m_charset);
	}
	
	private static final List<File> collectShpFiles(File path) throws IOException {
		List<File> shpFiles = Shapefile.traverseShpFiles(path).toList();
		s_logger.info("loading: nfiles={}", shpFiles.size());
		
		return shpFiles;
	}
}
