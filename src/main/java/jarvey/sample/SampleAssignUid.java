package jarvey.sample;

import java.io.File;

import jarvey.JarveySession;
import jarvey.SpatialDataFrame;
import jarvey.command.JarveyLocalCommand;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
public class SampleAssignUid {
	public static final void main(String[] args) throws Exception {
		JarveyLocalCommand.configureLog4j(new File("."), false);
		JarveySession jarvey = JarveySession.builder()
											.appName("assign_uid")
											.master("local[7]")
											.getOrCreate();
		
		String keyDsId = "구역/읍면동";
		
		SpatialDataFrame keySds = jarvey.read().dataset(keyDsId)
										.assignUid("uid");
		keySds.drop("the_geom").show(5, false);
		
		jarvey.spark().stop();
	}
}
