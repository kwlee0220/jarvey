// Generated from JarveyTypeExpr.g4 by ANTLR 4.8

package jarvey.support.typeexpr;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link JarveyTypeExprParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface JarveyTypeExprVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#columnExprList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnExprList(JarveyTypeExprParser.ColumnExprListContext ctx);
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#columnExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnExpr(JarveyTypeExprParser.ColumnExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#typeExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeExpr(JarveyTypeExprParser.TypeExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#geomtryTypeExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGeomtryTypeExpr(JarveyTypeExprParser.GeomtryTypeExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#sridSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSridSpec(JarveyTypeExprParser.SridSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link JarveyTypeExprParser#nullSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullSpec(JarveyTypeExprParser.NullSpecContext ctx);
}