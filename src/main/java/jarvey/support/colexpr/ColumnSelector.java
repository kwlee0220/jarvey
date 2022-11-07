package jarvey.support.colexpr;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
import jarvey.type.JarveySchema;

import utils.CIString;
import utils.Utilities;
import utils.func.Tuple;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnSelector {
	private final Map<String,JarveySchema> m_schemas;
	private final String m_colExpr;
	
	public static final ColumnSelector fromExpression(String columnExpression) {
		return new ColumnSelector(Maps.newHashMap(), columnExpression);
	}
	
	public static final ColumnSelector from(JarveySchema jschema, String columnExpr) {
		return new ColumnSelector(Maps.newHashMap(), columnExpr)
					.addOrReplaceDataset("", jschema);
	}
	
	private ColumnSelector(Map<String,JarveySchema> namespaces, String columnExpression) {
		Utilities.checkNotNullArgument(columnExpression, "column expression is null");
		
		m_schemas = namespaces;
		m_colExpr = columnExpression;
	}
	
	public JarveySchema getSourceSchema(String alias) {
		Utilities.checkNotNullArgument(alias, "RecordSchema alias is null");
		
		return m_schemas.get(alias);
	}
	
	public ColumnSelector addOrReplaceDataset(String alias, JarveySchema schema) {
		Utilities.checkNotNullArgument(alias, "StructType alias is null");
		Utilities.checkNotNullArgument(schema, "StructType is null");
		
		Map<String,JarveySchema> added = Maps.newHashMap(m_schemas);
		added.put(alias, schema);
		return new ColumnSelector(added, m_colExpr);
	}
	
	public String getColumnExpression() {
		return m_colExpr;
	}
	
	public Set<SelectedColumnInfo> select() throws ColumnSelectionException {
		return parseColumnExpression(m_colExpr);
	}
	
	public Column[] selectColumnExpr(Dataset<Row> df) throws ColumnSelectionException {
		Map<String,Dataset<Row>> dfs = Maps.newHashMap();
		dfs.put("", df);
		return selectColumnExpr(dfs);
	}
	
	public Column[] selectColumnExpr(Map<String,Dataset<Row>> dfs) throws ColumnSelectionException {
		return FStream.from(select())
						.map(info -> info.toColumnExpr(dfs))
						.toArray(Column.class);
	}
	
	@Override
	public String toString() {
		return m_colExpr;
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
			
			FStream.from(jschema.getColumnAll())
					.forEach(col -> m_selecteds.add(new SelectedColumnInfo(ns, col.getName().get())));
			
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
			
			Set<CIString> keys = FStream.from(colNameList).map(CIString::of).toSet();
			FStream.from(jschema.getColumnAll())
					.filter(c -> !keys.contains(c.getName()))
					.map(col -> new SelectedColumnInfo(ns, col.getName().get()))
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
			
			SelectedColumnInfo info = FStream.from(jschema.getColumnAll())
											.filter(c -> c.getName().equals(colName))
											.map(c -> new SelectedColumnInfo(ns, c.getName().get()))
											.findFirst()
											.getOrThrow(() -> {
												String details = String.format("unknown column: [%s:%s], schema=%s",
																				ns, colName, jschema);
												throw new ColumnSelectionException(details);
											});
			if ( alias != null ) {
				info.setAlias(alias);
			}
			m_selecteds.add(info);
		}
	}
}
