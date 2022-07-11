/**
 * 
 */
package jarvey.datasource.shp;

import org.apache.spark.sql.connector.read.InputPartition;


/**
 *
 * @author Kang-Woo Lee
 */
public class ShpInputPartition implements InputPartition {
	private static final long serialVersionUID = 1L;

	@Override
	public String[] preferredLocations() {
		return new String[0];
	}
}
