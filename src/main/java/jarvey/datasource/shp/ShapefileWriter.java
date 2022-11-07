package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDumper;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import jarvey.SpatialDataFrame;
import jarvey.datasource.DatasetException;
import jarvey.type.JarveySchema;

import utils.func.Try;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileWriter {
	private final File m_outputDir;
	private final int m_srid;
	private final Charset m_charset;
	private String m_sftName = "main";
	private long m_maxShpSize = -1;
	private long m_maxDbfSize = -1;
	private boolean m_force = false;
	
	public static ShapefileWriter get(File outputDir, int srid, Charset charset) {
		return new ShapefileWriter(outputDir, srid, charset);
	}
	
	private ShapefileWriter(File outputDir, int srid, Charset charset) {
		m_outputDir = outputDir;
		m_srid = srid;
		m_charset = charset;
	}
	
	public ShapefileWriter setTypeName(String sftName) {
		m_sftName = sftName;
		return this;
	}
	
	public ShapefileWriter setMaxShpFileSize(long size) {
		m_maxShpSize = size;
		return this;
	}
	
	public ShapefileWriter setMaxDbfFileSize(long size) {
		m_maxDbfSize = size;
		return this;
	}
	
	public ShapefileWriter setForce(boolean flag) {
		m_force = flag;
		return this;
	}

	public long write(SpatialDataFrame sds) {
		SimpleFeatureType sfType = ShapefileDataSets.toSimpleFeatureType(m_sftName, m_srid, sds.getJarveySchema());
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		
		JarveySchema jschema = sds.getJarveySchema();
		List<SimpleFeature> featureList = FStream.from(sds.toLocalIterator())
												.map(row -> jschema.deserialize(row))
												.map(cols -> builder.buildFeature(null, cols))
												.toList();
		SimpleFeatureCollection sfColl = new ListFeatureCollection(sfType, featureList);
		
		try {
			if ( m_force ) {
				Try.run(() -> FileUtils.forceDelete(m_outputDir));
			}
			
			FileUtils.forceMkdir(m_outputDir);
			ShapefileDumper dumper = new ShapefileDumper(m_outputDir);
			dumper.setCharset(m_charset);
			if ( m_maxShpSize > 0 ) {
				dumper.setMaxShpSize(m_maxShpSize);
			}
			if ( m_maxDbfSize > 0 ) {
				dumper.setMaxDbfSize(m_maxDbfSize);
			}
			dumper.dump(sfColl);
			
			return featureList.size();
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to write SimpleFeatures: dir=" + m_outputDir, e);
		}
	}
}
