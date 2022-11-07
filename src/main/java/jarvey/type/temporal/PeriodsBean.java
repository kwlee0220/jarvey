package jarvey.type.temporal;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import com.google.common.collect.Lists;

import utils.stream.FStream;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class PeriodsBean {
	public static final Encoder<PeriodsBean> ENCODER = Encoders.bean(PeriodsBean.class);
	
	private List<Long> m_firstTss;
	private List<Long> m_lastTss;
	
	public PeriodsBean() {
		m_firstTss = Lists.newArrayList();
		m_lastTss = Lists.newArrayList();
	}
	
	public PeriodBean[] toPeriodBeans() {
		return FStream.from(m_firstTss)
						.zipWith(FStream.from(m_lastTss))
						.map(t -> new PeriodBean(t._1, t._2))
						.toArray(PeriodBean.class);
	}
	
	public int length() {
		return m_firstTss.size();
	}
	
	public Long[] getFirstTss() {
		return m_firstTss.toArray(new Long[length()]);
	}
	public void setFirstTss(Long[] tss) {
		m_firstTss = Arrays.asList(tss);
	}
	
	public Long[] getLastTss() {
		return m_lastTss.toArray(new Long[length()]);
	}
	public void setLastTss(Long[] tss) {
		m_lastTss = Arrays.asList(tss);
	}
	
	public void add(long firstTs, long lastTs) {
		m_firstTss.add(firstTs);
		m_lastTss.add(lastTs);
	}
	
	public void addAll(PeriodsBean bean) {
		m_firstTss.addAll(bean.m_firstTss);
		m_lastTss.addAll(bean.m_lastTss);
	}
}
