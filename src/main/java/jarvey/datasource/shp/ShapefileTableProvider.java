package jarvey.datasource.shp;


import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import jarvey.datasource.DatasetException;
import jarvey.type.JarveyDataTypes;

import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee
 */
public class ShapefileTableProvider implements TableProvider {
	public ShapefileTableProvider() { }

	@Override
	public StructType inferSchema(CaseInsensitiveStringMap options) {
		File start = new File(options.get("path"));
		try {
			SimpleFeatureType sfType = ShapefileDataSets.getSimpleFeatureType(start);
			return FStream.from(sfType.getAttributeDescriptors())
							.map(desc -> toField(desc))
							.foldLeft(new StructType(), (acc, field) -> acc.add(field));
		}
		catch ( IOException e ) {
			throw new DatasetException("fails to read Shapefile: " + start);
		}
		catch ( NumberFormatException e ) {
			throw new DatasetException("invalid EPSG code, tried to load from file=" + start, e);
		}
	}

	@Override
	public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
		return new ShapefileTable(schema, properties);
	}

	@Override
	public boolean supportsExternalMetadata() {
		return true;
	}
	
	static StructField toField(AttributeDescriptor desc) {
		Class<?> instCls = desc.getType().getBinding();
		DataType sparkType = JarveyDataTypes.fromJavaClass(instCls).getSparkType();
		
		return DataTypes.createStructField(desc.getLocalName(), sparkType, true);
	}
}
