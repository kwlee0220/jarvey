// Generated from JarveyTypeExpr.g4 by ANTLR 4.8

package jarvey.support.typeexpr;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class JarveyTypeExprParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31, 
		T__31=32, T__32=33, T__33=34, T__34=35, T__35=36, ID=37, INT=38, STRING=39, 
		LINE_COMMENT=40, COMMENT=41, WS=42;
	public static final int
		RULE_columnExprList = 0, RULE_columnExpr = 1, RULE_typeExpr = 2, RULE_geomtryTypeExpr = 3, 
		RULE_sridSpec = 4, RULE_nullSpec = 5;
	private static String[] makeRuleNames() {
		return new String[] {
			"columnExprList", "columnExpr", "typeExpr", "geomtryTypeExpr", "sridSpec", 
			"nullSpec"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "','", "'not'", "'null'", "'String'", "'Long'", "'Integer'", "'Int'", 
			"'Short'", "'Byte'", "'Double'", "'Float'", "'Binary'", "'Boolean'", 
			"'Date'", "'Timestamp'", "'CalendarInterval'", "'Envelope'", "'GridCell'", 
			"'Vector'", "'TemporalPoint'", "'Array'", "'<'", "'>'", "'Geometry'", 
			"'Point'", "'MultiPoint'", "'LineString'", "'MultiLineString'", "'Polygon'", 
			"'MultiPolygon'", "'GeometryCollection'", "'('", "')'", "'nullable'", 
			"'non-null'", "'not-null'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, "ID", "INT", "STRING", "LINE_COMMENT", "COMMENT", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "JarveyTypeExpr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public JarveyTypeExprParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class ColumnExprListContext extends ParserRuleContext {
		public List<ColumnExprContext> columnExpr() {
			return getRuleContexts(ColumnExprContext.class);
		}
		public ColumnExprContext columnExpr(int i) {
			return getRuleContext(ColumnExprContext.class,i);
		}
		public ColumnExprListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnExprList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitColumnExprList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnExprListContext columnExprList() throws RecognitionException {
		ColumnExprListContext _localctx = new ColumnExprListContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_columnExprList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(12);
			columnExpr();
			setState(17);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(13);
				match(T__0);
				setState(14);
				columnExpr();
				}
				}
				setState(19);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnExprContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(JarveyTypeExprParser.ID, 0); }
		public TypeExprContext typeExpr() {
			return getRuleContext(TypeExprContext.class,0);
		}
		public ColumnExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitColumnExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnExprContext columnExpr() throws RecognitionException {
		ColumnExprContext _localctx = new ColumnExprContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_columnExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(20);
			match(ID);
			setState(21);
			typeExpr();
			setState(24);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(22);
				match(T__1);
				setState(23);
				match(T__2);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeExprContext extends ParserRuleContext {
		public GeomtryTypeExprContext geomtryTypeExpr() {
			return getRuleContext(GeomtryTypeExprContext.class,0);
		}
		public SridSpecContext sridSpec() {
			return getRuleContext(SridSpecContext.class,0);
		}
		public TypeExprContext typeExpr() {
			return getRuleContext(TypeExprContext.class,0);
		}
		public NullSpecContext nullSpec() {
			return getRuleContext(NullSpecContext.class,0);
		}
		public TypeExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitTypeExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeExprContext typeExpr() throws RecognitionException {
		TypeExprContext _localctx = new TypeExprContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_typeExpr);
		int _la;
		try {
			setState(56);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__3:
				enterOuterAlt(_localctx, 1);
				{
				setState(26);
				match(T__3);
				}
				break;
			case T__4:
				enterOuterAlt(_localctx, 2);
				{
				setState(27);
				match(T__4);
				}
				break;
			case T__5:
				enterOuterAlt(_localctx, 3);
				{
				setState(28);
				match(T__5);
				}
				break;
			case T__6:
				enterOuterAlt(_localctx, 4);
				{
				setState(29);
				match(T__6);
				}
				break;
			case T__7:
				enterOuterAlt(_localctx, 5);
				{
				setState(30);
				match(T__7);
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 6);
				{
				setState(31);
				match(T__8);
				}
				break;
			case T__9:
				enterOuterAlt(_localctx, 7);
				{
				setState(32);
				match(T__9);
				}
				break;
			case T__10:
				enterOuterAlt(_localctx, 8);
				{
				setState(33);
				match(T__10);
				}
				break;
			case T__11:
				enterOuterAlt(_localctx, 9);
				{
				setState(34);
				match(T__11);
				}
				break;
			case T__12:
				enterOuterAlt(_localctx, 10);
				{
				setState(35);
				match(T__12);
				}
				break;
			case T__13:
				enterOuterAlt(_localctx, 11);
				{
				setState(36);
				match(T__13);
				}
				break;
			case T__14:
				enterOuterAlt(_localctx, 12);
				{
				setState(37);
				match(T__14);
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 13);
				{
				setState(38);
				match(T__15);
				}
				break;
			case T__23:
			case T__24:
			case T__25:
			case T__26:
			case T__27:
			case T__28:
			case T__29:
			case T__30:
				enterOuterAlt(_localctx, 14);
				{
				setState(39);
				geomtryTypeExpr();
				setState(41);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__31) {
					{
					setState(40);
					sridSpec();
					}
				}

				}
				break;
			case T__16:
				enterOuterAlt(_localctx, 15);
				{
				setState(43);
				match(T__16);
				}
				break;
			case T__17:
				enterOuterAlt(_localctx, 16);
				{
				setState(44);
				match(T__17);
				}
				break;
			case T__18:
				enterOuterAlt(_localctx, 17);
				{
				setState(45);
				match(T__18);
				}
				break;
			case T__19:
				enterOuterAlt(_localctx, 18);
				{
				setState(46);
				match(T__19);
				}
				break;
			case T__20:
				enterOuterAlt(_localctx, 19);
				{
				setState(47);
				match(T__20);
				setState(48);
				match(T__21);
				setState(49);
				typeExpr();
				setState(52);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(50);
					match(T__0);
					setState(51);
					nullSpec();
					}
				}

				setState(54);
				match(T__22);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GeomtryTypeExprContext extends ParserRuleContext {
		public GeomtryTypeExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_geomtryTypeExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitGeomtryTypeExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GeomtryTypeExprContext geomtryTypeExpr() throws RecognitionException {
		GeomtryTypeExprContext _localctx = new GeomtryTypeExprContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_geomtryTypeExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(58);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29) | (1L << T__30))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SridSpecContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(JarveyTypeExprParser.INT, 0); }
		public SridSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sridSpec; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitSridSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SridSpecContext sridSpec() throws RecognitionException {
		SridSpecContext _localctx = new SridSpecContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_sridSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(60);
			match(T__31);
			setState(61);
			match(INT);
			setState(62);
			match(T__32);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NullSpecContext extends ParserRuleContext {
		public NullSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullSpec; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof JarveyTypeExprVisitor ) return ((JarveyTypeExprVisitor<? extends T>)visitor).visitNullSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullSpecContext nullSpec() throws RecognitionException {
		NullSpecContext _localctx = new NullSpecContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_nullSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(64);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__33) | (1L << T__34) | (1L << T__35))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3,E\4\2\t\2\4\3\t\3"+
		"\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\3\2\3\2\3\2\7\2\22\n\2\f\2\16\2\25\13"+
		"\2\3\3\3\3\3\3\3\3\5\3\33\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\5\4,\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4"+
		"\67\n\4\3\4\3\4\5\4;\n\4\3\5\3\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\2\2\b\2\4"+
		"\6\b\n\f\2\4\3\2\32!\3\2$&\2T\2\16\3\2\2\2\4\26\3\2\2\2\6:\3\2\2\2\b<"+
		"\3\2\2\2\n>\3\2\2\2\fB\3\2\2\2\16\23\5\4\3\2\17\20\7\3\2\2\20\22\5\4\3"+
		"\2\21\17\3\2\2\2\22\25\3\2\2\2\23\21\3\2\2\2\23\24\3\2\2\2\24\3\3\2\2"+
		"\2\25\23\3\2\2\2\26\27\7\'\2\2\27\32\5\6\4\2\30\31\7\4\2\2\31\33\7\5\2"+
		"\2\32\30\3\2\2\2\32\33\3\2\2\2\33\5\3\2\2\2\34;\7\6\2\2\35;\7\7\2\2\36"+
		";\7\b\2\2\37;\7\t\2\2 ;\7\n\2\2!;\7\13\2\2\";\7\f\2\2#;\7\r\2\2$;\7\16"+
		"\2\2%;\7\17\2\2&;\7\20\2\2\';\7\21\2\2(;\7\22\2\2)+\5\b\5\2*,\5\n\6\2"+
		"+*\3\2\2\2+,\3\2\2\2,;\3\2\2\2-;\7\23\2\2.;\7\24\2\2/;\7\25\2\2\60;\7"+
		"\26\2\2\61\62\7\27\2\2\62\63\7\30\2\2\63\66\5\6\4\2\64\65\7\3\2\2\65\67"+
		"\5\f\7\2\66\64\3\2\2\2\66\67\3\2\2\2\678\3\2\2\289\7\31\2\29;\3\2\2\2"+
		":\34\3\2\2\2:\35\3\2\2\2:\36\3\2\2\2:\37\3\2\2\2: \3\2\2\2:!\3\2\2\2:"+
		"\"\3\2\2\2:#\3\2\2\2:$\3\2\2\2:%\3\2\2\2:&\3\2\2\2:\'\3\2\2\2:(\3\2\2"+
		"\2:)\3\2\2\2:-\3\2\2\2:.\3\2\2\2:/\3\2\2\2:\60\3\2\2\2:\61\3\2\2\2;\7"+
		"\3\2\2\2<=\t\2\2\2=\t\3\2\2\2>?\7\"\2\2?@\7(\2\2@A\7#\2\2A\13\3\2\2\2"+
		"BC\t\3\2\2C\r\3\2\2\2\7\23\32+\66:";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}