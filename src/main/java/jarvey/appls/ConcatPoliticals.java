package jarvey.appls;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import jarvey.JarveySession;
import jarvey.SpatialDataset;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatPoliticals {
	private static final String SID = "district_sd";
	private static final String SGG = "district_sgg";
	private static final String EMD = "district_emd";
	private static final String LI = "district_li";
	private static final String OUTPUT = "district_combined";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		JarveySession jarvey = JarveySession.builder()
											.appName("load_shapefile")
											.master("local[5]")
											.getOrCreate();
		
		StopWatch watch = StopWatch.start();
		
		SpatialDataset li = jarvey.read().dataset(LI);
		SpatialDataset emd = jarvey.read().dataset(EMD)
									.withColumnRenamed("the_geom", "emd_the_geom");
		SpatialDataset sgg = jarvey.read().dataset(SGG)
									.select("sig_cd", "sig_kor_nm");
		SpatialDataset sd = jarvey.read().dataset(SID)
									.select("ctprvn_cd", "ctp_kor_nm");
		
		SpatialDataset res;
		res = li.withRegularColumn("emd_cd2", substring(col("li_cd"), 0, 8));
		res = res.join(emd, col("emd_cd2").equalTo(col("emd_cd")), "right");
		res = res.withRegularColumn("bjd_nm", when(col("li_cd").isNotNull(),
											concat(col("emd_kor_nm"), lit(" "), col("li_kor_nm")))
										.otherwise(col("emd_kor_nm")))
				.withRegularColumn("bjd_cd", when(col("li_cd").isNotNull(), col("li_cd"))
										.otherwise(concat(col("emd_cd"), lit("00"))))
				.withRegularColumn("the_geom", when(col("li_cd").isNull(), col("emd_the_geom")));
		res = res.select(col("the_geom"), col("bjd_cd"), col("bjd_nm"), col("emd_cd"),
						col("emd_kor_nm").as("emd_nm"), col("li_cd"), col("li_kor_nm").as("li_nm"));
		res = res.withRegularColumn("sig_cd2", substring(col("bjd_cd"), 0, 5))
					.join(sgg, col("sig_cd2").equalTo(col("sig_cd")), "inner");
		res = res.withRegularColumn("bjd_nm", when(col("sig_kor_nm").notEqual("세종특별자치시"),
											concat(col("sig_kor_nm"), lit(" "), col("bjd_nm"))))
					.select(col("the_geom"), col("bjd_cd"), col("bjd_nm"),
							col("sig_cd2").as("sgg_cd"), col("sig_kor_nm").as("sgg_nm"),
							col("emd_cd"), col("emd_nm"), col("li_cd"), col("li_nm"));
		res = res.withRegularColumn("sid_cd2", substring(col("bjd_cd"), 0, 2))
					.join(sd, col("sid_cd2").equalTo(col("ctprvn_cd")), "inner")
					.withRegularColumn("bjd_nm", concat(col("ctp_kor_nm"), lit(" "), col("bjd_nm")))
					.select(col("the_geom"), col("bjd_cd"), col("bjd_nm"),
							col("ctprvn_cd").as("sid_cd"), col("ctp_kor_nm").as("sid_nm"),
							col("sgg_cd"), col("sgg_nm"), col("emd_cd"), col("emd_nm"),
							col("li_cd"), col("li_nm"));
		res = res.coalesce(1);
		
		res.writeSpatial().mode(SaveMode.Overwrite).dataset(OUTPUT);
		res.printSchema();
		res.drop("the_geom").show(5);
		
		jarvey.spark().stop();
	}
}
