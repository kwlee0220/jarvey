package jarvey.optor.geom.join;

import jarvey.optor.geom.SpatialRelation;
import jarvey.quadtree.Enveloped;
import jarvey.type.JarveySchema;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface SpatialJoinMatcher<T extends GeometryHolder & Enveloped> {
	public SpatialRelation toSpatialRelation();
	
	public void open(JarveySchema leftSchema, JarveySchema rightSchema);
	public void close();
	
	// Nested-Loop 기반의 조인 매칭 방법시 활용되는 인터페이스들
	
	/**
	 * Inner 클러스터 ({@code innerCluster})에 저장된 레코드들 중에서 주어진 outer 레코드의 영역과
	 * 매치되는 레코드들의 스트림을 반환한다.
	 * 
	 * @param keyWsg84		검색에 사용할 사각 영역 (EPSG:4326)
	 * @param outer			검색에 사용할 outer record 객체.
	 * @param slut	검색 대상의 공간 데이터 검색 테이블
	 * @return	매치가되는 inner 레코드들의 리스트.
	 */
	public FStream<T> match(T outer, SpatialLookupTable<T> slut);

	public String toStringExpr();
}
