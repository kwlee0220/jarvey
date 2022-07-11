/**
 * 
 */
package jarvey.datasource.shp;


import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jarvey.JarveyRuntimeException;
import jarvey.datasource.DatasetException;
import jarvey.support.TypeUtils;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import utils.func.FOption;
import utils.geo.Shapefile;
import utils.geo.util.CRSUtils;
import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee
 */
public class ShpTableProvider implements TableProvider {
	private static final Logger s_logger = LoggerFactory.getLogger(ShpPartitionReader.class);
	
	private GeometryType m_defGeomType = GeometryType.of(Geometries.GEOMETRY, 0);
	
	public ShpTableProvider() { }

	@Override
	public StructType inferSchema(CaseInsensitiveStringMap options) {
		Shapefile shpFile = loadShapefile(options);
		
		try {
			SimpleFeatureType sfType = shpFile.getSimpleFeatureType();
			
			Geometry firstGeom = shpFile.streamGeometries().findFirst().get();
			int srid = getSrid(sfType);
			m_defGeomType = JarveyDataType.fromJavaClass(firstGeom.getClass())
											.asGeometryType()
											.newGeometryType(srid);
			s_logger.info("loading shapefile: default geometry={}", m_defGeomType);

			return FStream.from(sfType.getAttributeDescriptors())
							.map(desc -> toField(desc, m_defGeomType))
							.foldLeft(new StructType(), (acc, field) -> acc.add(field));
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to read Shapefile: " + shpFile);
		}
	}

	@Override
	public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
		return new ShpTable(schema, m_defGeomType, properties);
	}

	@Override
	public boolean supportsExternalMetadata() {
		return true;
	}
	
	static StructField toField(AttributeDescriptor desc, GeometryType srid) {
		Class<?> instCls = desc.getType().getBinding();
		DataType dtype = TypeUtils.toDataType(instCls);
		
		return DataTypes.createStructField(desc.getLocalName(), dtype, true);
	}
	
	static Shapefile loadShapefile(CaseInsensitiveStringMap options) {
		File start = new File(options.get("path"));
		try {
			List<File> shpFiles = Shapefile.traverseShpFiles(start).toList();
			if ( shpFiles.isEmpty() ) {
				throw new IllegalArgumentException("no Shapefiles to read: path=" + start);
			}
			
			Charset charset = FOption.ofNullable(options.get("charset"))
									.map(Charset::forName)
									.getOrElse(StandardCharsets.UTF_8);
			return Shapefile.of(shpFiles.get(0), charset);
		}
		catch ( IOException e ) {
			throw new JarveyRuntimeException("fails to load Shapefile: file=" + start,  e);
		}
	}
	
	static SimpleFeatureType loadSimpleFeatureType(CaseInsensitiveStringMap options) {
		try {
			return loadShapefile(options).getSimpleFeatureType();
		}
		catch ( IOException e ) {
			File start = new File(options.get("path"));
			throw new JarveyRuntimeException("fails to load Shapefile: file=" + start,  e);
		}
	}
	
	static int getSrid(SimpleFeatureType sfType) {
		try {
			CoordinateReferenceSystem crs = sfType.getCoordinateReferenceSystem();
			if ( crs == null ) {
				return 0;
			}
			String epsg = CRSUtils.toEPSG(crs);
			if ( epsg == null ) {
				return 0;
			}
			try {
				return (crs != null) ? Integer.parseInt(epsg.substring(5)) : 0;
			}
			catch ( NumberFormatException e ) {
				throw new JarveyRuntimeException("invalid EPSG: " + epsg);
			}
		}
		catch ( FactoryException e ) {
			throw new JarveyRuntimeException("fails to load EPSG: cause=" + e);
		}
	}
	
	static int loadSrids(SimpleFeatureType sfType) {
		try {
			CoordinateReferenceSystem crs = sfType.getCoordinateReferenceSystem();
			if ( crs == null ) {
				return 0;
			}
			String epsg = CRSUtils.toEPSG(crs);
			if ( epsg == null ) {
				return 0;
			}
			try {
				return (crs != null) ? Integer.parseInt(epsg.substring(5)) : 0;
			}
			catch ( NumberFormatException e ) {
				throw new JarveyRuntimeException("invalid EPSG: " + epsg);
			}
		}
		catch ( FactoryException e ) {
			throw new JarveyRuntimeException("fails to load EPSG: cause=" + e);
		}
	}
}
