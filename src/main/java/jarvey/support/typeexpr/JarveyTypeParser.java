package jarvey.support.typeexpr;

import java.util.Set;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.google.common.collect.Sets;

import jarvey.support.colexpr.SelectedColumnInfo;
import jarvey.support.typeexpr.JarveyTypeExprParser.GeomtryTypeExprContext;
import jarvey.support.typeexpr.JarveyTypeExprParser.NullSpecContext;
import jarvey.support.typeexpr.JarveyTypeExprParser.SridSpecContext;
import jarvey.support.typeexpr.JarveyTypeExprParser.TypeExprContext;
import jarvey.type.EnvelopeType;
import jarvey.type.GeometryType;
import jarvey.type.GridCellType;
import jarvey.type.JarveyArrayType;
import jarvey.type.JarveyDataType;
import jarvey.type.JarveyDataTypes;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JarveyTypeParser {
	public static JarveyDataType parseTypeExpr(String typeExprString) {
		typeExprString = typeExprString.trim();
		JarveyTypeExprLexer lexer = new JarveyTypeExprLexer(new ANTLRInputStream(typeExprString));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		JarveyTypeExprParser parser = new JarveyTypeExprParser(tokens);
		
		ParseTree tree = parser.typeExpr();
		Visitor visitor = new Visitor();
		return (JarveyDataType)visitor.visit(tree);
	}
	
	static class Visitor extends JarveyTypeExprBaseVisitor<Object> {
		private Set<SelectedColumnInfo> m_selecteds = Sets.newLinkedHashSet();
		
		Set<SelectedColumnInfo> getSelectedColumnInfos() {
			return m_selecteds;
		}

		@Override
		public Object visitTypeExpr(TypeExprContext ctx) {
			String typeName = ctx.getChild(0).getText();
			GeomtryTypeExprContext geomTypeCxt = ctx.getChild(GeomtryTypeExprContext.class, 0);
			if ( geomTypeCxt != null ) {
				SridSpecContext sridCtx = ctx.getChild(SridSpecContext.class, 0);
				int srid = ( sridCtx != null ) ? (int)visitSridSpec(sridCtx) : 0;
				return GeometryType.fromString(typeName, srid);
			}
			else {
				switch ( typeName ) {
					case "String":
						return JarveyDataTypes.String_Type;
					case "Long":
						return JarveyDataTypes.Long_Type;
					case "Integer":
					case "Int":
						return JarveyDataTypes.Integer_Type;
					case "Short":
						return JarveyDataTypes.Short_Type;
					case "Byte":
						return JarveyDataTypes.Byte_Type;
					case "Double":
						return JarveyDataTypes.Double_Type;
					case "Float":
						return JarveyDataTypes.Float_Type;
					case "Binary":
						return JarveyDataTypes.Binary_Type;
					case "Boolean":
						return JarveyDataTypes.Boolean_Type;
					case "Date":
						return JarveyDataTypes.Date_Type;
					case "Timestamp":
						return JarveyDataTypes.Timestamp_Type;
					case "CalendarInterval":
						return JarveyDataTypes.CalendarInterval_Type;
					case "Envelope":
						return EnvelopeType.get();
					case "GridCell":
						return GridCellType.get();
					case "Vector":
						return JarveyDataTypes.Vector_Type;
						
					case "TemporalPoint":
						return JarveyDataTypes.Temporal_Point_Type;
						
					case "Array":
						TypeExprContext elmTypeCxt = ctx.getChild(TypeExprContext.class, 0);
						JarveyDataType elmType = (JarveyDataType)visitTypeExpr(elmTypeCxt);
						
						NullSpecContext nullCtx = ctx.getChild(NullSpecContext.class, 0);
						boolean nullable = true;
						if ( nullCtx != null ) {
							nullable = (boolean)visitNullSpec(nullCtx);
						}
						return JarveyArrayType.of(elmType, nullable);
					default:
						throw new AssertionError("unknown type name: " + typeName);
				}
			}
		}
		
		@Override
		public Object visitSridSpec(SridSpecContext ctx) {
			return Integer.parseInt(ctx.getChild(1).getText());
		}
		
		@Override public Object visitNullSpec(NullSpecContext ctx) {
			switch ( ctx.getChild(0).getText() ) {
				case "nullable":
					return true;
				case "non-null":
					return false;
				case "not-null":
					return false;
				default:
					throw new AssertionError();
			}
		}
		
		
		
		
		
		
		
		
		
		
		
		
//		@Override
//		public Object visitSelectionExpr(SelectionExprContext ctx) throws ColumnSelectionException {
//			for ( ColumnExprContext exprCtx: ctx.columnExpr() ) {
//				visitColumnExpr(exprCtx);
//			}
//			return null;
//		}
//		
//		@Override
//		public Object visitColumnExpr(ColumnExprContext ctx) throws ColumnSelectionException {
//			ParseTree child = ctx.getChild(0);
//			if ( child instanceof FullColNameContext ) {
//				visitFullColName((FullColNameContext)child);
//			}
//			else if ( child instanceof FullColNameListContext ) {
//				visitFullColNameList((FullColNameListContext)child);
//			}
//			else if ( child instanceof AllContext ) {
//				visitAll((AllContext)child);
//			}
//			else if ( child instanceof AllButContext ) {
//				visitAllBut((AllButContext)child);
//			}
//			return null;
//		}
//		
//		@Override
//		public Object visitAll(AllContext ctx) throws ColumnSelectionException {
//			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
//			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
//			
//			SpatialDataset sds = m_datasets.get(ns);
//			if ( sds == null ) {
//				String details = String.format("unknown namespace: namespace='%s'", ns);
//				throw new ColumnSelectionException(details);
//			}
//			
//			FStream.of(sds.schema().fields())
//					.forEach(col -> m_selecteds.add(new SelectedColumnInfo(ns, col.name())));
//			
//			return null;
//		}
//		
//		@Override
//		public Object visitAllBut(AllButContext ctx) throws ColumnSelectionException {
//			IdListContext idListCtx = ctx.getChild(IdListContext.class, 0);
//			List<String> colNameList = visitIdList(idListCtx);
//			
//			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
//			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
//			SpatialDataset jschema = m_datasets.get(ns);
//			if ( jschema == null ) {
//				String details = String.format("unknown namespace: namespace='%s'", ns);
//				throw new ColumnSelectionException(details);
//			}
//			
//			Set<String> keys = FStream.from(colNameList).map(String::toLowerCase).toSet();
//			FStream.of(jschema.schema().fields())
//					.filter(c -> !keys.contains(c.name().toLowerCase()))
//					.map(col -> new SelectedColumnInfo(ns, col.name()))
//					.forEach(m_selecteds::add);
//			
//			return null;
//		}
//		
//		@Override
//		public Object visitFullColNameList(FullColNameListContext ctx) {
//			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
//			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
//			
//			ColNameListContext colNameListCtx = ctx.getChild(ColNameListContext.class, 0);
//			visitColNameList(colNameListCtx).stream()
//					.forEach(t -> handleLiteral(ns, t._1, t._2));
//			return null;
//		}
//		
//		@Override
//		public Object visitFullColName(FullColNameContext ctx) throws ColumnSelectionException {
//			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
//			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
//			
//			ColNameContext colNameCtx = ctx.getChild(ColNameContext.class, 0);
//			Tuple<String,String> colName = visitColName(colNameCtx);
//			
//			handleLiteral(ns, colName._1, colName._2);
//			return null;
//		}
//		
//		@Override
//		public List<Tuple<String,String>> visitColNameList(ColNameListContext ctx) {
//			return ctx.children.stream()
//						.filter(x -> x instanceof ColNameContext)
//						.map(x -> visitColName((ColNameContext)x))
//						.collect(Collectors.toList());
//		}
//		
//		@Override
//		public Tuple<String,String> visitColName(ColNameContext ctx) {
//			String colName = ctx.getChild(0).getText();
//			AliasContext aliasCtx = ctx.getChild(AliasContext.class, 0);
//			if ( aliasCtx != null ) {
//				String alias = aliasCtx.getChild(1).getText();
//				return Tuple.of(colName, alias);
//			}
//			else {
//				return Tuple.of(colName, null);
//			}
//		}
//		
//		@Override
//		public List<String> visitIdList(IdListContext ctx) {
//			return FStream.from(ctx.children)
//							.map(ParseTree::getText)
//							.filter(text -> !text.equals(","))
//							.toList();
//		}
//		
//		private void handleLiteral(String ns, String colName, String alias)
//			throws ColumnSelectionException {
//			SpatialDataset jschema = m_datasets.get(ns);
//			if ( jschema == null ) {
//				String details = String.format("unknown namespace: namespace='%s'", ns);
//				throw new ColumnSelectionException(details);
//			}
//			
//			SelectedColumnInfo info = FStream.of(jschema.schema().fields())
//											.filter(f -> f.name().equals(colName))
//											.map(field -> new SelectedColumnInfo(ns, field.name()))
//											.findFirst()
//											.getOrThrow(() -> {
//												String details = String.format("unknown column: [%s:%s], schema=%s",
//																				ns, colName, jschema);
//												throw new ColumnSelectionException(details);
//											});
//			if ( alias != null ) {
//				info.setAlias(alias);
//			}
//			m_selecteds.add(info);
//		}
	}
}
