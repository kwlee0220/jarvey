package jarvey.join;

import java.io.Serializable;

import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final SpatialJoinOptions DEFAULT
							= new SpatialJoinOptions(FOption.empty(), FOption.empty());
	
	private final FOption<String> m_joinExpr;
	private final FOption<String> m_outputCols;
	
	private SpatialJoinOptions(FOption<String> joinExpr, FOption<String> outputCols) {
		m_joinExpr = joinExpr;
		m_outputCols = outputCols;
	}
	
	public static SpatialJoinOptions OUTPUT(String outCols) {
		Utilities.checkNotNullArgument(outCols, "output columns are null");
		
		return new SpatialJoinOptions(FOption.empty(), FOption.of(outCols));
	}
	
	public static SpatialJoinOptions WITHIN_DISTANCE(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		FOption<String> joinExprStr = FOption.of(SpatialRelation.WITHIN_DISTANCE(dist))
											.map(SpatialRelation::toStringExpr);
		return new SpatialJoinOptions(joinExprStr, FOption.empty());
	}
	
	public FOption<String> joinExpr() {
		return m_joinExpr;
	}
	
	public SpatialJoinOptions joinExpr(String expr) {
		Utilities.checkNotNullArgument(expr, "join expression");
		
		return new SpatialJoinOptions(FOption.of(expr), m_outputCols);
	}
	
	public SpatialJoinOptions joinExpr(SpatialRelation rel) {
		Utilities.checkNotNullArgument(rel, "join expression");

		return joinExpr(rel.toStringExpr());
	}
	
	public SpatialJoinOptions withinDistance(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		return joinExpr(SpatialRelation.WITHIN_DISTANCE(dist));
	}
	
	public FOption<String> outputColumns() {
		return m_outputCols;
	}
	
	public SpatialJoinOptions outputColumns(String outCols) {
		return new SpatialJoinOptions(m_joinExpr, FOption.of(outCols));
	}
}
