package jarvey.optor.geom.join;

import org.locationtech.jts.geom.Envelope;

import jarvey.quadtree.Enveloped;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface SpatialLookupTable<T extends Enveloped> {
	/**
	 * 클러스터에 포함된 모든 레코드에 대해 주어진 키와 겹치는 레코드 스트림을 반환한다.
	 * 
	 * @param range84			질의 영역, 위경도(WGS84) 좌표계 사용
	 * @param dropDuplicates	복제본 제외 여부
	 * @return	질의에 포함된 레코드들의 스트림.
	 */
	public FStream<T> query(Envelope range84, boolean dropDuplicates);
}