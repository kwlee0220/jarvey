package jarvey.optor.geom.join;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import jarvey.optor.geom.SpatialRelation;
import jarvey.support.colexpr.JoinColumnSelector;
import jarvey.support.colexpr.SelectedColumnInfo;
import jarvey.type.JarveySchema;

import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final String INNER_JOIN = "innerJoin";
	public static final String SEMI_JOIN = "semiJoin";
	public static final String DIFFERENCE_JOIN = "difference";
	public static final String ARC_CLIP = "arcClip";
	private static final String[] JOIN_TYPES = new String[]{INNER_JOIN, SEMI_JOIN, DIFFERENCE_JOIN, ARC_CLIP};
	public static final SpatialJoinOptions DEFAULT
							= new SpatialJoinOptions(SpatialRelation.INTERSECTS, INNER_JOIN, null, false);
	
	private final SpatialRelation m_joinExpr;
	@Nullable private final String m_outputCols;
	private final String m_joinType;
	private final boolean m_negated;
	
	private SpatialJoinOptions(SpatialRelation joinExpr, String joinType, String outputCols, boolean negated) {
		Utilities.checkNotNullArgument(joinExpr);
		Utilities.checkNotNullArgument(joinType);
		
		m_joinExpr = joinExpr;
		m_joinType = joinType;
		m_outputCols = outputCols;
		m_negated = negated;
	}
	
	public static SpatialJoinOptions OUTPUT(String outCols) {
		Utilities.checkNotNullArgument(outCols, "output columns are null");
		
		return DEFAULT.outputColumns(outCols);
	}
	public FOption<String> outputColumns() {
		return FOption.ofNullable(m_outputCols);
	}
	public SpatialJoinOptions outputColumns(String outCols) {
		return new SpatialJoinOptions(m_joinExpr, m_joinType, outCols, m_negated);
	}
	
	public static SpatialJoinOptions WITHIN_DISTANCE(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		return DEFAULT.joinExpr(SpatialRelation.WITHIN_DISTANCE(dist));
	}
	public SpatialJoinOptions withinDistance(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		return joinExpr(SpatialRelation.WITHIN_DISTANCE(dist));
	}
	
	public SpatialRelation joinExpr() {
		return m_joinExpr;
	}
	public SpatialJoinOptions joinExpr(SpatialRelation expr) {
		Utilities.checkNotNullArgument(expr, "join expression");
		
		return new SpatialJoinOptions(expr, m_joinType, m_outputCols, m_negated);
	}

	
	public static SpatialJoinOptions SEMI_JOIN() { return DEFAULT.joinType(SEMI_JOIN); }
	public static SpatialJoinOptions INNER_JOIN() { return DEFAULT.joinType(INNER_JOIN); }
	public static SpatialJoinOptions ARC_CLIP() { return DEFAULT.joinType(ARC_CLIP); }
	public static SpatialJoinOptions DIFFERENCE_JOIN() { return DEFAULT.joinType(DIFFERENCE_JOIN); }
	public String joinType() {
		return m_joinType;
	}
	public SpatialJoinOptions joinType(String type) {
		Utilities.checkNotNullArgument(type, "join type");
		
		return new SpatialJoinOptions(m_joinExpr, type, m_outputCols, m_negated);
	}

	public boolean isNegated() {
		return m_negated;
	}
	public SpatialJoinOptions negated() {
		return new SpatialJoinOptions(m_joinExpr, m_joinType, m_outputCols, true);
	}
	
	public JarveySchema calcOutputJarveySchema(JarveySchema leftSchema, JarveySchema rightSchema) {
		JoinColumnSelector selector = outputColumns()
										.map(expr -> new JoinColumnSelector(leftSchema, rightSchema, expr))
										.getOrNull();
		if ( selector == null ) {
			throw new IllegalArgumentException("output columns not specified");
		}
		
		Set<String> outputCols = Sets.newHashSet();
		selector.findColumnInfo("left", leftSchema.assertDefaultGeometryColumnInfo().getName())
				.map(SelectedColumnInfo::getOutputColumnName)
				.ifPresent(outputCols::add);
		selector.findColumnInfo("right", rightSchema.assertDefaultGeometryColumnInfo().getName())
				.map(SelectedColumnInfo::getOutputColumnName)
				.ifPresent(outputCols::add);
		return selector.getOutputJarveySchema();
	}
}
