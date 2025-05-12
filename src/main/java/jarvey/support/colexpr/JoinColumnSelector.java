package jarvey.support.colexpr;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import utils.CIString;
import utils.Tuple;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

import jarvey.support.RecordLite;
import jarvey.support.colexpr.ColumnSelectionExprParser.AliasContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.AllButContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.AllContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.ColNameContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.ColNameListContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.ColumnExprContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.FullColNameContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.FullColNameListContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.IdListContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.NamespaceContext;
import jarvey.support.colexpr.ColumnSelectionExprParser.SelectionExprContext;
import jarvey.type.JarveyColumn;
import jarvey.type.JarveySchema;
import jarvey.type.JarveySchemaBuilder;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinColumnSelector implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Map<String,JarveySchema> m_schemas;
	private final String m_colExpr;
	private final JarveySchema m_outputJSchema;
	private final Set<SelectedColumnInfo> m_selection;
	
	public JoinColumnSelector(JarveySchema left, JarveySchema right, String columnExpression) {
		Utilities.checkNotNullArgument(columnExpression, "column expression is null");
		
		m_schemas = Maps.newHashMap();
		m_schemas.put("left", left);
		m_schemas.put("right", right);
		m_colExpr = columnExpression;
		
		m_selection = parseColumnExpression(m_colExpr);
		JarveySchemaBuilder builder
				= FStream.from(m_selection)
						.fold(JarveySchema.builder(), (bldr, cinfo) -> addJarveyColumn(bldr, cinfo));
		
		// Default geometry column을 설정한다.
		//
		// 오른쪽 join SpatialDataFrame의 default Geometry가 조인 결과에 포함되는가 확인하여
		// 포함된 경우, 이것은 조인 결과 SpatialDataFrame의 default geometry로 설정한다.
		CIString rightDefGeomCol = right.getDefaultGeometryColumn().getName();
		FStream.from(m_selection)
				.filter(info -> rightDefGeomCol.equals(info.getColumnName()))
				.forEach(info -> builder.setDefaultGeometryColumn(info.getOutputColumnName()));
		// 왼쪽 join SpatialDataFrame의 default Geometry가 조인 결과에 포함되는가 확인하여
		// 포함된 경우, 이것은 조인 결과 SpatialDataFrame의 default geometry로 설정한다.
		// 만일 이전 과정에서 기 default geometry가 설정된 경우에는 이전 것은 무시된다.
		CIString leftDefGeomCol = left.getDefaultGeometryColumn().getName();
		FStream.from(m_selection)
				.filter(info -> leftDefGeomCol.equals(info.getColumnName()))
				.forEach(info -> builder.setDefaultGeometryColumn(info.getOutputColumnName()));
		m_outputJSchema = builder.build();
	}
	
	public String getColumnExpression() {
		return m_colExpr;
	}
	
	public Set<SelectedColumnInfo> getColumnSelection() {
		return m_selection;
	}
	
	public JarveySchema getOutputJarveySchema() {
		return m_outputJSchema;
	}
	
	public FOption<SelectedColumnInfo> findColumnInfo(String ns, String colName) {
		return FStream.from(m_selection)
						.findFirst(info -> info.getNamespace().equals(ns)
									&& info.getColumnName().equalsIgnoreCase(colName));
	}
	
	public RecordLite select(JarveySchema leftSchema, RecordLite left,
						JarveySchema rightSchema, RecordLite right) throws ColumnSelectionException {
		Set<SelectedColumnInfo> columnInfos = parseColumnExpression(m_colExpr);
		
		int idx = 0;
		Object[] values = new Object[columnInfos.size()];
		for ( SelectedColumnInfo cinfo: columnInfos ) {
			if ( cinfo.getNamespace().equals("right") ) {
				int colIdx = rightSchema.getColumn(cinfo.getColumnName()).getIndex();
				values[idx] = right.get(colIdx);
			}
			else {
				int colIdx = leftSchema.getColumn(cinfo.getColumnName()).getIndex();
				values[idx] = left.get(colIdx);
			}
			++idx;
		}
		
		return RecordLite.of(values);
	}
	
	public Row select(JarveySchema leftSchema, Row left, JarveySchema rightSchema, Row right)
		throws ColumnSelectionException {
		Set<SelectedColumnInfo> columnInfos = parseColumnExpression(m_colExpr);
		
		int idx = 0;
		Object[] values = new Object[columnInfos.size()];
		for ( SelectedColumnInfo cinfo: columnInfos ) {
			if ( cinfo.getNamespace().equals("right") ) {
				int colIdx = rightSchema.getColumn(cinfo.getColumnName()).getIndex();
				values[idx] = right.get(colIdx);
			}
			else {
				int colIdx = leftSchema.getColumn(cinfo.getColumnName()).getIndex();
				values[idx] = left.get(colIdx);
			}
			++idx;
		}
		
		return RowFactory.create(values);
	}
	
	@Override
	public String toString() {
		return m_colExpr;
	}
	
	private JarveySchemaBuilder addJarveyColumn(JarveySchemaBuilder builder, SelectedColumnInfo cinfo) {
		JarveySchema jschema = m_schemas.getOrDefault(cinfo.getNamespace(), m_schemas.get("left"));
		JarveyColumn jcol = jschema.getColumn(cinfo.getColumnName());
		String name = (cinfo.getAlias() != null) ? cinfo.getAlias() : cinfo.getColumnName();
		
		return builder.addJarveyColumn(name, jcol.getJarveyDataType());
	}
	
	private Set<SelectedColumnInfo> parseColumnExpression(String colExprString) {
		colExprString = colExprString.trim();
		if ( colExprString.length() == 0 ) {
			return Sets.newHashSet();
		}
		
		ColumnSelectionExprLexer lexer = new ColumnSelectionExprLexer(new ANTLRInputStream(colExprString));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		ColumnSelectionExprParser parser = new ColumnSelectionExprParser(tokens);
		
		ParseTree tree = parser.selectionExpr();
		Visitor visitor = new Visitor();
		visitor.visit(tree);
		
		return visitor.getSelectedColumnInfos();
	}
	
	class Visitor extends ColumnSelectionExprBaseVisitor<Object> {
		private Set<SelectedColumnInfo> m_selecteds = Sets.newLinkedHashSet();
		
		Set<SelectedColumnInfo> getSelectedColumnInfos() {
			return m_selecteds;
		}
		
		@Override
		public Object visitSelectionExpr(SelectionExprContext ctx) throws ColumnSelectionException {
			for ( ColumnExprContext exprCtx: ctx.columnExpr() ) {
				visitColumnExpr(exprCtx);
			}
			return null;
		}
		
		@Override
		public Object visitColumnExpr(ColumnExprContext ctx) throws ColumnSelectionException {
			ParseTree child = ctx.getChild(0);
			if ( child instanceof FullColNameContext ) {
				visitFullColName((FullColNameContext)child);
			}
			else if ( child instanceof FullColNameListContext ) {
				visitFullColNameList((FullColNameListContext)child);
			}
			else if ( child instanceof AllContext ) {
				visitAll((AllContext)child);
			}
			else if ( child instanceof AllButContext ) {
				visitAllBut((AllButContext)child);
			}
			return null;
		}
		
		@Override
		public Object visitAll(AllContext ctx) throws ColumnSelectionException {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			JarveySchema jschema = m_schemas.get(ns);
			if ( jschema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			FStream.of(jschema.getSchema().fields())
					.forEach(col -> m_selecteds.add(new SelectedColumnInfo(ns, col.name())));
			
			return null;
		}
		
		@Override
		public Object visitAllBut(AllButContext ctx) throws ColumnSelectionException {
			IdListContext idListCtx = ctx.getChild(IdListContext.class, 0);
			List<String> colNameList = visitIdList(idListCtx);
			
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			JarveySchema jschema = m_schemas.get(ns);
			if ( jschema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			Set<String> keys = FStream.from(colNameList).map(String::toLowerCase).toSet();
			FStream.of(jschema.getSchema().fields())
					.filter(c -> !keys.contains(c.name().toLowerCase()))
					.map(col -> new SelectedColumnInfo(ns, col.name()))
					.forEach(m_selecteds::add);
			
			return null;
		}
		
		@Override
		public Object visitFullColNameList(FullColNameListContext ctx) {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			ColNameListContext colNameListCtx = ctx.getChild(ColNameListContext.class, 0);
			visitColNameList(colNameListCtx).stream()
					.forEach(t -> handleLiteral(ns, t._1, t._2));
			return null;
		}
		
		@Override
		public Object visitFullColName(FullColNameContext ctx) throws ColumnSelectionException {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			ColNameContext colNameCtx = ctx.getChild(ColNameContext.class, 0);
			Tuple<String,String> colName = visitColName(colNameCtx);
			
			handleLiteral(ns, colName._1, colName._2);
			return null;
		}
		
		@Override
		public List<Tuple<String,String>> visitColNameList(ColNameListContext ctx) {
			return ctx.children.stream()
						.filter(x -> x instanceof ColNameContext)
						.map(x -> visitColName((ColNameContext)x))
						.collect(Collectors.toList());
		}
		
		@Override
		public Tuple<String,String> visitColName(ColNameContext ctx) {
			String colName = ctx.getChild(0).getText();
			AliasContext aliasCtx = ctx.getChild(AliasContext.class, 0);
			if ( aliasCtx != null ) {
				String alias = aliasCtx.getChild(1).getText();
				return Tuple.of(colName, alias);
			}
			else {
				return Tuple.of(colName, null);
			}
		}
		
		@Override
		public List<String> visitIdList(IdListContext ctx) {
			return FStream.from(ctx.children)
							.map(ParseTree::getText)
							.filter(text -> !text.equals(","))
							.toList();
		}
		
		private void handleLiteral(String ns, String colName, String alias)
			throws ColumnSelectionException {
			JarveySchema jschema = m_schemas.get(ns);
			if ( jschema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			SelectedColumnInfo info = FStream.of(jschema.getSchema().fields())
											.filter(f -> f.name().equals(colName))
											.map(field -> new SelectedColumnInfo(ns, field.name()))
											.findFirst()
											.getOrThrow(() -> {
												String details = String.format("unknown column: [%s:%s], schema=%s",
																				ns, colName, jschema.getSchema());
												throw new ColumnSelectionException(details);
											});
			if ( alias != null ) {
				info.setAlias(alias);
			}
			m_selecteds.add(info);
		}
	}
}
