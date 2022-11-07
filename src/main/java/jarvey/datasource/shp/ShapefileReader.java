package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveyRuntimeException;
import jarvey.type.JarveySchema;

import utils.LoggerSettable;
import utils.StopWatch;
import utils.Throwables;
import utils.geo.SimpleFeatureDataStore;
import utils.stream.FStream;


/**
 *
 * @author Kang-Woo Lee
 */
class ShapefileReader implements PartitionReader<InternalRow>, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileReader.class);
	
	private final JarveySchema m_jschema;
	private final File m_path;
	private final Charset m_charset;
	@Nullable private final int[] m_mapping;
	
	private boolean m_closed = false;
	private SimpleFeatureDataStore m_sfdStore;
	private SimpleFeatureIterator m_iter;
	private SimpleFeature m_feature;
	private int m_count = 0;
	private StopWatch m_watch;
	private Logger m_logger = s_logger;
	
	ShapefileReader(JarveySchema jschema, File path, String charsetName, int[] mapping) {
		m_jschema = jschema;
		m_path = path;
		m_charset = Charset.forName(charsetName);
		m_mapping = mapping;
	}
	
	ShapefileReader(JarveySchema jschema, File path, String charsetName) {
		m_jschema = jschema;
		m_path = path;
		m_charset = Charset.forName(charsetName);
		m_mapping = null;
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
	
	public int getReadCount() {
		return m_count;
	}
	
	@Override
	public boolean next() throws IOException {
		if ( m_closed ) {
			throw new JarveyRuntimeException("already closed");
		}
		if ( m_iter == null ) {
			m_watch = StopWatch.start();
			loadShapefile();
		}
		
		if ( !m_iter.hasNext() ) {
			m_watch.stop();
			if ( getLogger().isInfoEnabled() ) {
				getLogger().info("read: {}", getMetricString());
			}
				
			return false;
		}
		m_feature = m_iter.next();
		++m_count;
		
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
		
		if ( m_feature.getAttribute(0) == null ) {
			getLogger().warn("found NULL Geometry at {}-th feature, file={}", m_count, m_path);
		}
		
		List<Object> cols = m_feature.getAttributes();
		List<Object> projected = (m_mapping != null) ? FStream.of(m_mapping).map(cols::get).toList() : cols;
		List<Object> serialized = m_jschema.serializeToInternal(projected);
		return new GenericInternalRow(serialized.toArray());
	}
	
	public String getMetricString() {
		double velo = m_count / m_watch.getElapsedInFloatingSeconds();
		return String.format("file=%s count=%d elapsed=%s, velo=%.1f/s",
								m_path.getName(), m_count, m_watch.getElapsedMillisString(), velo);
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	private final void loadShapefile() throws IOException {
		if ( getLogger().isDebugEnabled() ) {
			getLogger().debug("reading shapefile: {}", m_path);
		}
		
		m_sfdStore = SimpleFeatureDataStore.of(m_path, m_charset);
		m_iter = m_sfdStore.read().features();
	}
}
