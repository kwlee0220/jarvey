package jarvey.optor;

import java.util.function.Function;

import org.apache.spark.api.java.JavaRDD;

import jarvey.JarveySession;
import jarvey.support.RecordLite;
import jarvey.type.JarveySchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RDDFunction extends Function<JavaRDD<RecordLite>,JavaRDD<RecordLite>> {
	/**
	 * RDD 함수를 초기화시킨다.
	 * 
	 * @param jarvey	jarvey 객체.
	 * @param inputSchema	입력 레코드세트의 스키마 정보.
	 * @param deserializeCols 입력 레코드에서 deserialize된 컬럼명들
	 */
	public void initialize(JarveySession jarvey, JarveySchema inputSchema);
	
	/**
	 * 본 함수의 결과 RDD의 스키마 정보를 반환한다.
	 * 
	 * 본 함수는 {@link #initialize(JarveySession, JarveySchema)}를 호출한 이후에 호출되어야 한다.
	 * 
	 * @return	결과 RDD의 스키마 정보.
	 * @throws IllegalStateException	초기화되지 않은 상태에서 호출된 경우.
	 */
	public JarveySchema getOutputSchema();
}
