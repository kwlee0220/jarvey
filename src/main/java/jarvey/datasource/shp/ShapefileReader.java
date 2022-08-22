package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveyRuntimeException;
import jarvey.type.JarveySchema;
import utils.Throwables;
import utils.geo.SimpleFeatureDataStore;


/**
 *
 * @author Kang-Woo Lee
 */
class ShapefileReader implements PartitionReader<InternalRow> {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileReader.class);
	
	private final JarveySchema m_jschema;
	private final File m_path;
	private final Charset m_charset;
	
	private boolean m_closed = false;
	private SimpleFeatureDataStore m_sfdStore;
	private SimpleFeatureIterator m_iter;
	private SimpleFeature m_feature;
	private int m_count = 0;
	
	ShapefileReader(JarveySchema jschema, File path, String charsetName) {
		m_jschema = jschema;
		m_path = path;
		m_charset = Charset.forName(charsetName);
	}
	
	@Override
	public void close() throws IOException {
		if ( m_iter != null ) {
			try {
				m_iter.close();
				m_iter = null;
				m_closed = true;
			}
			catch ( Exception e ) {
				Throwables.throwIfInstanceOf(e, IOException.class);
				throw Throwables.toRuntimeException(e);
			}
		}
	}
	
	@Override
	public boolean next() throws IOException {
		if ( m_closed ) {
			throw new JarveyRuntimeException("already closed");
		}
		if ( m_iter == null ) {
			loadShapefile();
		}
		
		if ( !m_iter.hasNext() ) {
			return false;
		}
		
		m_feature = m_iter.next();
		return true;
	}
	
	@Override
	public InternalRow get() {
		if ( m_closed ) {
			throw new JarveyRuntimeException("already closed");
		}
		if ( m_feature == null ) {
			throw new IllegalStateException("next() has not been called");
		}
		
		++m_count;
		if ( m_feature.getAttribute(0) == null ) {
			s_logger.warn("found NULL Geometry at {}-th feature, file={}", m_count, m_path);
		}
		
		List<Object> serialized = m_jschema.serialize(m_feature.getAttributes());
		return new GenericInternalRow(serialized.toArray());
	}
	
	private final void loadShapefile() throws IOException {
		s_logger.debug("loading shapefile: {}", m_path);
		m_sfdStore = SimpleFeatureDataStore.of(m_path, m_charset);
		
		m_iter = m_sfdStore.read().features();
	}
}
