package jarvey.udf;

import java.util.List;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.locationtech.jts.geom.Coordinate;

import com.google.common.collect.Lists;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class CoordinateArrayBean {
	public static final Encoder<CoordinateArrayBean> ENCODER = Encoders.bean(CoordinateArrayBean.class);
	
	private List<Coordinate> m_coordList;
	
	public CoordinateArrayBean() {
		m_coordList = Lists.newArrayList();
	}
	
	public int length() {
		return m_coordList.size();
	}
	
	public List<Coordinate> getCoordinateList() {
		return m_coordList;
	}
	
	public void setCoordinateList(List<Coordinate> coordList) {
		m_coordList = coordList;
	}
	
	public void add(Coordinate coord) {
		m_coordList.add(coord);
	}
	
	public void addAll(List<Coordinate> coordList) {
		m_coordList.addAll(coordList);
	}
}
