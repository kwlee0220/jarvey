package jarvey.datasource.shp;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import jarvey.datasource.DatasetException;
import jarvey.type.GeometryType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.geo.Shapefile;
import utils.geo.SimpleFeatureDataStore;
import utils.geo.util.CRSUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileDataSets {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileDataSets.class);
	
	private ShapefileDataSets() {
		throw new AssertionError("Should not be called here: class=" + getClass());
	}
	
	public static File findAnyShapefile(File start) throws IOException {
		return Shapefile.traverseShpFiles(start).findFirst().getOrNull();
	}
	
	public static SimpleFeatureType getSimpleFeatureType(File start) throws IOException {
		SimpleFeatureDataStore store = SimpleFeatureDataStore.of(findAnyShapefile(start));
		return store.getSchema();
	}
	
	public static GeometryType loadDefaultGeometryType(File start, int srid) {
		try {
			SimpleFeatureType sfType = getSimpleFeatureType(start);

			// srid는 인자로 전달된 값을 사용하되, 지정되지 않으면 shapefile의 geometry에서 얻는다.
			if ( srid <= 0 ) {
				srid = ShapefileDataSets.getSrid(sfType);
			}
			
			Class<?> geomCls = sfType.getGeometryDescriptor().getType().getBinding();
			return GeometryType.fromJavaClass(geomCls, srid);
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to read Shapefile", e);
		}
		catch ( NumberFormatException e ) {
			throw new DatasetException("invalid EPSG code, tried to load from file=" + start, e);
		}
		catch ( FactoryException e ) {
			throw new DatasetException("fails to load EPSG, tried to load from file=" + start, e);
		}
	}
	
	public static GeometryType loadDefaultGeometryType(CaseInsensitiveStringMap options) {
		File start = new File(options.get("path"));
		int srid = Integer.parseInt(options.getOrDefault("srid", "0"));
		
		return loadDefaultGeometryType(start, srid);
	}
	
	public static int getSrid(SimpleFeatureType sfType) throws FactoryException, NumberFormatException {
		return extractEpsgCode(getEpsg(sfType));
	}
	
	static int extractEpsgCode(String epsg) {
		return (epsg != null) ? Integer.parseInt(epsg.substring(5)) : 0;
	}
	
	static String getEpsg(SimpleFeatureType sfType) throws FactoryException {
		CoordinateReferenceSystem crs = sfType.getCoordinateReferenceSystem();
		if ( crs == null ) {
			return null;
		}
		return CRSUtils.toEPSG(crs);
	}
	
	public static SimpleFeatureType toSimpleFeatureType(String sfTypeName, int srid,
														JarveySchema jschema) {
		Utilities.checkNotNullArgument(sfTypeName);
		Utilities.checkNotNullArgument(srid);
		Utilities.checkNotNullArgument(jschema);
		
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(sfTypeName);
		builder.setSRS("EPSG:" + srid);
		
		Map<String,Integer> abbrs = Maps.newHashMap();
		FStream.from(jschema.getColumnAll())
				.forEach(jcol -> {
					String colName = jcol.getName().get();
					if ( colName.length() > 10 ) {
						colName = colName.substring(0, 9);
						int seqno = abbrs.getOrDefault(colName, 0);
						abbrs.put(colName, (seqno+1));
						colName += (""+seqno);
						
						s_logger.warn(String.format("truncate too long field name: %s->%s",
													jcol.getName().get(), colName));
					}
					
					JarveyDataType jtype = jcol.getJarveyDataType();
					if ( jtype.equals(JarveyDataTypes.String_Type) ) {
						builder.nillable(true).add(colName, String.class);
					}
					else if ( jtype.equals(JarveyDataTypes.Date_Type)
								|| jtype.equals(JarveyDataTypes.Timestamp_Type) ) {
						builder.add(colName, Date.class);
					}
					else {
						builder.add(colName, jtype.getJavaClass());
					}
				});
		return builder.buildFeatureType();
	}
	
//	/**
//	 * SimpleFeatureType로부터 RecordSchema 객체를 생성한다.
//	 * 
//	 * @param sfType	SimpleFeatureType 타입
//	 * @return	RecordSchema
//	 */
//	public static RecordSchema toRecordSchema(SimpleFeatureType sfType) {
//		Utilities.checkNotNullArgument(sfType);
//		
//		RecordSchema.Builder builder;
//		try {
//			CoordinateReferenceSystem crs = sfType.getCoordinateReferenceSystem();
//			String srid = (crs != null) ? CRSUtils.toEPSG(crs) : null;
//			
//			builder = RecordSchema.builder();
//			for ( AttributeDescriptor desc: sfType.getAttributeDescriptors() ) {
//				Class<?> instCls = desc.getType().getBinding();
//				DataType attrType = DataType.fromInstanceClass(instCls);
//				if ( attrType.isGeometryType() ) {
//					attrType = ((GeometryDataType)attrType).duplicate(srid);
//				}
//				builder.addColumn(desc.getLocalName(), attrType);
//			}
//		}
//		catch ( FactoryException e ) {
//			throw new DataSetException("fails to load CRS", e);
//		}
//		
//		return builder.build();
//	}
//	
//	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureType sfType,
//															FeatureIterator<SimpleFeature> iter) {
//		Utilities.checkNotNullArgument(sfType);
//		Utilities.checkNotNullArgument(iter);
//
//		return new SimpleFeatureRecordStream(sfType, iter);
//	}
//	
//	public static SimpleFeatureRecordStream toRecordStream(FeatureIterator<SimpleFeature> iter) {
//		Utilities.checkNotNullArgument(iter);
//		Utilities.checkArgument(iter.hasNext(), "FeatureIterator is empty");
//
//		return new SimpleFeatureRecordStream(iter);
//	}
//	
//	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureCollection sfColl) {
//		Utilities.checkNotNullArgument(sfColl);
//		
//		return toRecordStream(sfColl.getSchema(), sfColl.features());
//	}
//	
//	public static SimpleFeatureRecordStream toRecordStream(SimpleFeatureSource sfSrc)
//		throws IOException {
//		Utilities.checkNotNullArgument(sfSrc);
//		
//		return toRecordStream(sfSrc.getFeatures());
//	}
//	
//	public static List<SimpleFeature> toFeatureList(SimpleFeatureType sfType, RecordStream rset) {
//		Utilities.checkNotNullArgument(sfType);
//		Utilities.checkNotNullArgument(rset);
//		
//		List<SimpleFeature> features = Lists.newArrayList();
//		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
//		Record rec;
//		while ( (rec = rset.nextCopy()) != null ) {
//			SimpleFeature feature;
//			if ( rec instanceof SimpleFeatureRecord ) {
//				feature = ((SimpleFeatureRecord)rec).getSimpleFeature();
//			}
//			else {
//				feature = builder.buildFeature(null, rec.getAll());
//			}
//			features.add(feature);
//		}
//		
//		return features;
//	}
//	
//	public static List<SimpleFeature> toFeatureList(SimpleFeatureType sfType,
//													Iterable<Record> records) {
//		Utilities.checkNotNullArgument(sfType);
//		Utilities.checkNotNullArgument(records);
//		
//		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
//		return FStream.from(records).map(r -> builder.buildFeature(null, r.getAll())).toList();
//	}
//	
//	public static SimpleFeatureCollection toFeatureCollection(SimpleFeatureType sfType,
//																Iterable<Record> records) {
//		Utilities.checkNotNullArgument(sfType);
//		Utilities.checkNotNullArgument(records);
//		
//		return new ListFeatureCollection(sfType, toFeatureList(sfType, records));
//	}
}
