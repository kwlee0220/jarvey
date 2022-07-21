package jarvey.test;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import jarvey.type.JarveySchema;


public class TestReadCsvFile {
	public static final void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder()
											.appName("load_csv_files")
											.master("local[7]")
											.getOrCreate();
		JarveySession jarvey = JarveySession.of(spark);
		
		String schemaDDL = "trip_key string not null, device string not null, vin string not null,  "
							+ "car_type string not null, trans_reg_num string not null, "
							+ "carrier_code string not null, driver_code string not null, "
							+ "daily_mileage int not null, accum_mileage int not null, speed int not null, rpm int not null, "
							+ "brake byte not null, pos_x int not null, pos_y int not null, azimuth byte not null, "
							+ "accel_x float not null, accel_y float not null, comm_status string not null, "
							+ "area_code string not null, trans_loc_code string not null, trans_code string not null, "
							+ "ts string not null";
		StructType structType = (StructType)DataType.fromDDL(schemaDDL);
		
		Dataset<Row> df = spark.read()
								.option("header", "false")
								.option("delimiter", "|")
								.schema(structType)
								.csv("C:/Temp/data/DTG-r-00000-sub.csv");
		
		JarveySchema schema = JarveySchema.fromStructType(structType);
		SpatialDataset sds = new SpatialDataset(jarvey, df, schema)
								.point("pos_x", "pos_x", 4326, "the_geom")
								.drop("pos_x", "pos_y")
								.withRegularColumn("ts", to_timestamp(col("ts"), "yyMMddHHmmssSS"));
		sds.printSchema();
		
		sds.show(10);
		spark.stop();
	}
}
