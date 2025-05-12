package jarvey.quadtree;

import java.util.List;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.stream.FStream;
import utils.stream.KeyedGroups;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class PointerPartition implements Partition<Pointer> {
	private static final Logger s_logger = LoggerFactory.getLogger(PointerPartition.class);
//	private static final int MAX_MINI_PARTITION_SLOTS = 1024;
//	private static final int MAX_MINI_PARTITION_SLOTS = 256;
	private static final int MAX_MINI_PARTITION_SLOTS = 64;
	private static final float FILL_RATIO = 0.85f;
	
	private Envelope m_dataBounds;
	private List<Enveloped> m_slots;
	private int m_maxLength = MAX_MINI_PARTITION_SLOTS;
	private int m_ptrCount;
	private boolean m_grouped;
	
	public PointerPartition() {
		m_dataBounds = new Envelope();
		m_slots = Lists.newArrayList();
		m_ptrCount = 0;
		m_grouped = false;
	}
	
	private PointerPartition(Envelope dataBounds, int count, List<Pointer> ptrs,
							List<PointerGroup> grps) {
		m_dataBounds = dataBounds;
		m_slots = Lists.newArrayListWithExpectedSize(ptrs.size() + grps.size());
		m_slots.addAll(grps);
		m_slots.addAll(ptrs);
		m_ptrCount = count;
		m_grouped = grps.size() > 0;
	}
	
	public int getMaxSlots() {
		return m_maxLength;
	}

	@Override
	public Envelope getBounds() {
		return m_dataBounds;
	}
	
	@Override
	public boolean add(Pointer value) {
		return add(value, true);
	}
	
	@Override
	public boolean add(Pointer value, boolean reserveForSpeed) {
		if ( m_slots.size() < m_maxLength ) {
			m_dataBounds.expandToInclude(value.getEnvelope84());
			m_slots.add(value);
			++m_ptrCount;
			
			return true;
		}
		
		// 본 partition에서 수용할 수 있는 수의 데이터가 넘어선 경우.
		
		// 입력 데이터가 point가 아닌 경우는 삽입에 실패했다고 반환한다.
		if ( value.getEnvelope84().getArea() > 0 ) {
			return false;
		}	
		
		int prevSlotCount = m_slots.size();
		m_slots = compact();
		double fillRatio = (double)m_slots.size()/m_maxLength;
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug(String.format("compact partition: %d -> %d (%.2f)",
										prevSlotCount, m_slots.size(), fillRatio));
		}
		
		// compact 작업 이후에도 어느정도 수 이상의로 slot을 차지하게 되면
		// leaf-node가 split되도록 삽입이 실패된 것으로 처리한다.
		if ( (reserveForSpeed && fillRatio > FILL_RATIO)
			||  m_slots.size() > m_maxLength) {
			return false;
		}
		
		m_dataBounds.expandToInclude(value.getEnvelope84());
		m_slots.add(value);
		++m_ptrCount;
		
		return true;
	}
	
	@Override
	public boolean tryExpand() {
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("{} slot expanded: {} -> {}", getClass().getSimpleName(),
							m_maxLength, m_maxLength*2);
		}
		m_maxLength *= 2;
		return true;
	}

	@Override
	public int size() {
		return m_ptrCount;
	}

	@Override
	public FStream<Pointer> values() {
		if ( m_grouped ) {
			return FStream.from(m_slots)
							.flatMap(slot -> {
								if ( slot instanceof PointerGroup ) {
									return ((PointerGroup)slot).stream();
								}
								else {
									return FStream.of((Pointer)slot);
								}
							});
		}
		else {
			return FStream.from(m_slots).map(o -> (Pointer)o);
		}
	}
	
	@Override
	public String toString() {
		return String.format("Pointers(%d/%d,%.1f%%)", m_ptrCount, m_maxLength,
								(double)m_ptrCount/m_maxLength*100);
	}
	
	private List<Enveloped> compact() {
		List<Enveloped> compacteds = Lists.newArrayList();
		KeyedGroups<Envelope,Enveloped> groups = FStream.from(m_slots)
														.tagKey(Enveloped::getEnvelope84)
														.groupByKey();
		for ( Envelope key: groups.keySet() ) {
			List<Enveloped> group = groups.get(key);
			
			if ( group.size() == 1 ) {
				compacteds.add(group.get(0));
			}
			else {
				m_grouped = true;
				int[] indexes = FStream.from(group)
									.flatMap(v -> {
										if ( v instanceof Pointer ) {
											return FStream.of((Pointer)v);
										}
										else if ( v instanceof PointerGroup ) {
											return ((PointerGroup)v).stream();
										}
										else {
											throw new AssertionError();
										}
									})
									.mapToInt(ptr -> ptr.index())
									.toArray();
				compacteds.add(new PointerGroup(key, indexes));
			}	
		}
		
		return compacteds;
	}
}