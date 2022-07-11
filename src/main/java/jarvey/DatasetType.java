package jarvey;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum DatasetType {
	HEAP,
	CSV,
	SPATIAL_CLUSTER;
	
	public String id() {
		return name();
	}

	public static DatasetType fromString(String id) {
		return DatasetType.valueOf(id.toUpperCase());
	}
}
