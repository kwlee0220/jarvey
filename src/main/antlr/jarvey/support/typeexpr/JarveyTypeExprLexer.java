// Generated from JarveyTypeExpr.g4 by ANTLR 4.8

package jarvey.support.typeexpr;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class JarveyTypeExprLexer extends Lexer {
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
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
			"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
			"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "T__24", 
			"T__25", "T__26", "T__27", "T__28", "T__29", "T__30", "T__31", "T__32", 
			"T__33", "T__34", "T__35", "ID", "INT", "ID_LETTER", "DIGIT", "STRING", 
			"ESC", "LINE_COMMENT", "COMMENT", "WS"
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


	public JarveyTypeExprLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "JarveyTypeExpr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2,\u01b4\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\3\2\3\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3"+
		"\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3"+
		"\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3"+
		"\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3"+
		"\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3"+
		"\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3"+
		"\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3"+
		"\26\3\26\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3"+
		"\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3"+
		"\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3"+
		"\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3"+
		"\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3"+
		"\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3"+
		" \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3!\3!\3\"\3\"\3#\3#\3#\3#\3#"+
		"\3#\3#\3#\3#\3$\3$\3$\3$\3$\3$\3$\3$\3$\3%\3%\3%\3%\3%\3%\3%\3%\3%\3&"+
		"\3&\3&\7&\u0178\n&\f&\16&\u017b\13&\3\'\6\'\u017e\n\'\r\'\16\'\u017f\3"+
		"(\3(\3)\3)\3*\3*\3*\7*\u0189\n*\f*\16*\u018c\13*\3*\3*\3+\3+\3+\3,\3,"+
		"\3,\3,\7,\u0197\n,\f,\16,\u019a\13,\3,\3,\3,\3,\3-\3-\3-\3-\7-\u01a4\n"+
		"-\f-\16-\u01a7\13-\3-\3-\3-\3-\3-\3.\6.\u01af\n.\r.\16.\u01b0\3.\3.\5"+
		"\u018a\u0198\u01a5\2/\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27"+
		"\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33"+
		"\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'M(O\2Q\2S)U\2W*Y+[,\3\2\5\b\2&&"+
		"\61\61C\\aac|\u0082\0\b\2$$^^ddppttvv\5\2\13\f\17\17\"\"\2\u01b8\2\3\3"+
		"\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2"+
		"\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3"+
		"\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2"+
		"%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61"+
		"\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2"+
		"\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I"+
		"\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2S\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2"+
		"\2\2\3]\3\2\2\2\5_\3\2\2\2\7c\3\2\2\2\th\3\2\2\2\13o\3\2\2\2\rt\3\2\2"+
		"\2\17|\3\2\2\2\21\u0080\3\2\2\2\23\u0086\3\2\2\2\25\u008b\3\2\2\2\27\u0092"+
		"\3\2\2\2\31\u0098\3\2\2\2\33\u009f\3\2\2\2\35\u00a7\3\2\2\2\37\u00ac\3"+
		"\2\2\2!\u00b6\3\2\2\2#\u00c7\3\2\2\2%\u00d0\3\2\2\2\'\u00d9\3\2\2\2)\u00e0"+
		"\3\2\2\2+\u00ee\3\2\2\2-\u00f4\3\2\2\2/\u00f6\3\2\2\2\61\u00f8\3\2\2\2"+
		"\63\u0101\3\2\2\2\65\u0107\3\2\2\2\67\u0112\3\2\2\29\u011d\3\2\2\2;\u012d"+
		"\3\2\2\2=\u0135\3\2\2\2?\u0142\3\2\2\2A\u0155\3\2\2\2C\u0157\3\2\2\2E"+
		"\u0159\3\2\2\2G\u0162\3\2\2\2I\u016b\3\2\2\2K\u0174\3\2\2\2M\u017d\3\2"+
		"\2\2O\u0181\3\2\2\2Q\u0183\3\2\2\2S\u0185\3\2\2\2U\u018f\3\2\2\2W\u0192"+
		"\3\2\2\2Y\u019f\3\2\2\2[\u01ae\3\2\2\2]^\7.\2\2^\4\3\2\2\2_`\7p\2\2`a"+
		"\7q\2\2ab\7v\2\2b\6\3\2\2\2cd\7p\2\2de\7w\2\2ef\7n\2\2fg\7n\2\2g\b\3\2"+
		"\2\2hi\7U\2\2ij\7v\2\2jk\7t\2\2kl\7k\2\2lm\7p\2\2mn\7i\2\2n\n\3\2\2\2"+
		"op\7N\2\2pq\7q\2\2qr\7p\2\2rs\7i\2\2s\f\3\2\2\2tu\7K\2\2uv\7p\2\2vw\7"+
		"v\2\2wx\7g\2\2xy\7i\2\2yz\7g\2\2z{\7t\2\2{\16\3\2\2\2|}\7K\2\2}~\7p\2"+
		"\2~\177\7v\2\2\177\20\3\2\2\2\u0080\u0081\7U\2\2\u0081\u0082\7j\2\2\u0082"+
		"\u0083\7q\2\2\u0083\u0084\7t\2\2\u0084\u0085\7v\2\2\u0085\22\3\2\2\2\u0086"+
		"\u0087\7D\2\2\u0087\u0088\7{\2\2\u0088\u0089\7v\2\2\u0089\u008a\7g\2\2"+
		"\u008a\24\3\2\2\2\u008b\u008c\7F\2\2\u008c\u008d\7q\2\2\u008d\u008e\7"+
		"w\2\2\u008e\u008f\7d\2\2\u008f\u0090\7n\2\2\u0090\u0091\7g\2\2\u0091\26"+
		"\3\2\2\2\u0092\u0093\7H\2\2\u0093\u0094\7n\2\2\u0094\u0095\7q\2\2\u0095"+
		"\u0096\7c\2\2\u0096\u0097\7v\2\2\u0097\30\3\2\2\2\u0098\u0099\7D\2\2\u0099"+
		"\u009a\7k\2\2\u009a\u009b\7p\2\2\u009b\u009c\7c\2\2\u009c\u009d\7t\2\2"+
		"\u009d\u009e\7{\2\2\u009e\32\3\2\2\2\u009f\u00a0\7D\2\2\u00a0\u00a1\7"+
		"q\2\2\u00a1\u00a2\7q\2\2\u00a2\u00a3\7n\2\2\u00a3\u00a4\7g\2\2\u00a4\u00a5"+
		"\7c\2\2\u00a5\u00a6\7p\2\2\u00a6\34\3\2\2\2\u00a7\u00a8\7F\2\2\u00a8\u00a9"+
		"\7c\2\2\u00a9\u00aa\7v\2\2\u00aa\u00ab\7g\2\2\u00ab\36\3\2\2\2\u00ac\u00ad"+
		"\7V\2\2\u00ad\u00ae\7k\2\2\u00ae\u00af\7o\2\2\u00af\u00b0\7g\2\2\u00b0"+
		"\u00b1\7u\2\2\u00b1\u00b2\7v\2\2\u00b2\u00b3\7c\2\2\u00b3\u00b4\7o\2\2"+
		"\u00b4\u00b5\7r\2\2\u00b5 \3\2\2\2\u00b6\u00b7\7E\2\2\u00b7\u00b8\7c\2"+
		"\2\u00b8\u00b9\7n\2\2\u00b9\u00ba\7g\2\2\u00ba\u00bb\7p\2\2\u00bb\u00bc"+
		"\7f\2\2\u00bc\u00bd\7c\2\2\u00bd\u00be\7t\2\2\u00be\u00bf\7K\2\2\u00bf"+
		"\u00c0\7p\2\2\u00c0\u00c1\7v\2\2\u00c1\u00c2\7g\2\2\u00c2\u00c3\7t\2\2"+
		"\u00c3\u00c4\7x\2\2\u00c4\u00c5\7c\2\2\u00c5\u00c6\7n\2\2\u00c6\"\3\2"+
		"\2\2\u00c7\u00c8\7G\2\2\u00c8\u00c9\7p\2\2\u00c9\u00ca\7x\2\2\u00ca\u00cb"+
		"\7g\2\2\u00cb\u00cc\7n\2\2\u00cc\u00cd\7q\2\2\u00cd\u00ce\7r\2\2\u00ce"+
		"\u00cf\7g\2\2\u00cf$\3\2\2\2\u00d0\u00d1\7I\2\2\u00d1\u00d2\7t\2\2\u00d2"+
		"\u00d3\7k\2\2\u00d3\u00d4\7f\2\2\u00d4\u00d5\7E\2\2\u00d5\u00d6\7g\2\2"+
		"\u00d6\u00d7\7n\2\2\u00d7\u00d8\7n\2\2\u00d8&\3\2\2\2\u00d9\u00da\7X\2"+
		"\2\u00da\u00db\7g\2\2\u00db\u00dc\7e\2\2\u00dc\u00dd\7v\2\2\u00dd\u00de"+
		"\7q\2\2\u00de\u00df\7t\2\2\u00df(\3\2\2\2\u00e0\u00e1\7V\2\2\u00e1\u00e2"+
		"\7g\2\2\u00e2\u00e3\7o\2\2\u00e3\u00e4\7r\2\2\u00e4\u00e5\7q\2\2\u00e5"+
		"\u00e6\7t\2\2\u00e6\u00e7\7c\2\2\u00e7\u00e8\7n\2\2\u00e8\u00e9\7R\2\2"+
		"\u00e9\u00ea\7q\2\2\u00ea\u00eb\7k\2\2\u00eb\u00ec\7p\2\2\u00ec\u00ed"+
		"\7v\2\2\u00ed*\3\2\2\2\u00ee\u00ef\7C\2\2\u00ef\u00f0\7t\2\2\u00f0\u00f1"+
		"\7t\2\2\u00f1\u00f2\7c\2\2\u00f2\u00f3\7{\2\2\u00f3,\3\2\2\2\u00f4\u00f5"+
		"\7>\2\2\u00f5.\3\2\2\2\u00f6\u00f7\7@\2\2\u00f7\60\3\2\2\2\u00f8\u00f9"+
		"\7I\2\2\u00f9\u00fa\7g\2\2\u00fa\u00fb\7q\2\2\u00fb\u00fc\7o\2\2\u00fc"+
		"\u00fd\7g\2\2\u00fd\u00fe\7v\2\2\u00fe\u00ff\7t\2\2\u00ff\u0100\7{\2\2"+
		"\u0100\62\3\2\2\2\u0101\u0102\7R\2\2\u0102\u0103\7q\2\2\u0103\u0104\7"+
		"k\2\2\u0104\u0105\7p\2\2\u0105\u0106\7v\2\2\u0106\64\3\2\2\2\u0107\u0108"+
		"\7O\2\2\u0108\u0109\7w\2\2\u0109\u010a\7n\2\2\u010a\u010b\7v\2\2\u010b"+
		"\u010c\7k\2\2\u010c\u010d\7R\2\2\u010d\u010e\7q\2\2\u010e\u010f\7k\2\2"+
		"\u010f\u0110\7p\2\2\u0110\u0111\7v\2\2\u0111\66\3\2\2\2\u0112\u0113\7"+
		"N\2\2\u0113\u0114\7k\2\2\u0114\u0115\7p\2\2\u0115\u0116\7g\2\2\u0116\u0117"+
		"\7U\2\2\u0117\u0118\7v\2\2\u0118\u0119\7t\2\2\u0119\u011a\7k\2\2\u011a"+
		"\u011b\7p\2\2\u011b\u011c\7i\2\2\u011c8\3\2\2\2\u011d\u011e\7O\2\2\u011e"+
		"\u011f\7w\2\2\u011f\u0120\7n\2\2\u0120\u0121\7v\2\2\u0121\u0122\7k\2\2"+
		"\u0122\u0123\7N\2\2\u0123\u0124\7k\2\2\u0124\u0125\7p\2\2\u0125\u0126"+
		"\7g\2\2\u0126\u0127\7U\2\2\u0127\u0128\7v\2\2\u0128\u0129\7t\2\2\u0129"+
		"\u012a\7k\2\2\u012a\u012b\7p\2\2\u012b\u012c\7i\2\2\u012c:\3\2\2\2\u012d"+
		"\u012e\7R\2\2\u012e\u012f\7q\2\2\u012f\u0130\7n\2\2\u0130\u0131\7{\2\2"+
		"\u0131\u0132\7i\2\2\u0132\u0133\7q\2\2\u0133\u0134\7p\2\2\u0134<\3\2\2"+
		"\2\u0135\u0136\7O\2\2\u0136\u0137\7w\2\2\u0137\u0138\7n\2\2\u0138\u0139"+
		"\7v\2\2\u0139\u013a\7k\2\2\u013a\u013b\7R\2\2\u013b\u013c\7q\2\2\u013c"+
		"\u013d\7n\2\2\u013d\u013e\7{\2\2\u013e\u013f\7i\2\2\u013f\u0140\7q\2\2"+
		"\u0140\u0141\7p\2\2\u0141>\3\2\2\2\u0142\u0143\7I\2\2\u0143\u0144\7g\2"+
		"\2\u0144\u0145\7q\2\2\u0145\u0146\7o\2\2\u0146\u0147\7g\2\2\u0147\u0148"+
		"\7v\2\2\u0148\u0149\7t\2\2\u0149\u014a\7{\2\2\u014a\u014b\7E\2\2\u014b"+
		"\u014c\7q\2\2\u014c\u014d\7n\2\2\u014d\u014e\7n\2\2\u014e\u014f\7g\2\2"+
		"\u014f\u0150\7e\2\2\u0150\u0151\7v\2\2\u0151\u0152\7k\2\2\u0152\u0153"+
		"\7q\2\2\u0153\u0154\7p\2\2\u0154@\3\2\2\2\u0155\u0156\7*\2\2\u0156B\3"+
		"\2\2\2\u0157\u0158\7+\2\2\u0158D\3\2\2\2\u0159\u015a\7p\2\2\u015a\u015b"+
		"\7w\2\2\u015b\u015c\7n\2\2\u015c\u015d\7n\2\2\u015d\u015e\7c\2\2\u015e"+
		"\u015f\7d\2\2\u015f\u0160\7n\2\2\u0160\u0161\7g\2\2\u0161F\3\2\2\2\u0162"+
		"\u0163\7p\2\2\u0163\u0164\7q\2\2\u0164\u0165\7p\2\2\u0165\u0166\7/\2\2"+
		"\u0166\u0167\7p\2\2\u0167\u0168\7w\2\2\u0168\u0169\7n\2\2\u0169\u016a"+
		"\7n\2\2\u016aH\3\2\2\2\u016b\u016c\7p\2\2\u016c\u016d\7q\2\2\u016d\u016e"+
		"\7v\2\2\u016e\u016f\7/\2\2\u016f\u0170\7p\2\2\u0170\u0171\7w\2\2\u0171"+
		"\u0172\7n\2\2\u0172\u0173\7n\2\2\u0173J\3\2\2\2\u0174\u0179\5O(\2\u0175"+
		"\u0178\5O(\2\u0176\u0178\5Q)\2\u0177\u0175\3\2\2\2\u0177\u0176\3\2\2\2"+
		"\u0178\u017b\3\2\2\2\u0179\u0177\3\2\2\2\u0179\u017a\3\2\2\2\u017aL\3"+
		"\2\2\2\u017b\u0179\3\2\2\2\u017c\u017e\5Q)\2\u017d\u017c\3\2\2\2\u017e"+
		"\u017f\3\2\2\2\u017f\u017d\3\2\2\2\u017f\u0180\3\2\2\2\u0180N\3\2\2\2"+
		"\u0181\u0182\t\2\2\2\u0182P\3\2\2\2\u0183\u0184\4\62;\2\u0184R\3\2\2\2"+
		"\u0185\u018a\7)\2\2\u0186\u0189\5U+\2\u0187\u0189\13\2\2\2\u0188\u0186"+
		"\3\2\2\2\u0188\u0187\3\2\2\2\u0189\u018c\3\2\2\2\u018a\u018b\3\2\2\2\u018a"+
		"\u0188\3\2\2\2\u018b\u018d\3\2\2\2\u018c\u018a\3\2\2\2\u018d\u018e\7)"+
		"\2\2\u018eT\3\2\2\2\u018f\u0190\7^\2\2\u0190\u0191\t\3\2\2\u0191V\3\2"+
		"\2\2\u0192\u0193\7\61\2\2\u0193\u0194\7\61\2\2\u0194\u0198\3\2\2\2\u0195"+
		"\u0197\13\2\2\2\u0196\u0195\3\2\2\2\u0197\u019a\3\2\2\2\u0198\u0199\3"+
		"\2\2\2\u0198\u0196\3\2\2\2\u0199\u019b\3\2\2\2\u019a\u0198\3\2\2\2\u019b"+
		"\u019c\7\f\2\2\u019c\u019d\3\2\2\2\u019d\u019e\b,\2\2\u019eX\3\2\2\2\u019f"+
		"\u01a0\7\61\2\2\u01a0\u01a1\7,\2\2\u01a1\u01a5\3\2\2\2\u01a2\u01a4\13"+
		"\2\2\2\u01a3\u01a2\3\2\2\2\u01a4\u01a7\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a5"+
		"\u01a3\3\2\2\2\u01a6\u01a8\3\2\2\2\u01a7\u01a5\3\2\2\2\u01a8\u01a9\7,"+
		"\2\2\u01a9\u01aa\7\61\2\2\u01aa\u01ab\3\2\2\2\u01ab\u01ac\b-\2\2\u01ac"+
		"Z\3\2\2\2\u01ad\u01af\t\4\2\2\u01ae\u01ad\3\2\2\2\u01af\u01b0\3\2\2\2"+
		"\u01b0\u01ae\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2\u01b3"+
		"\b.\2\2\u01b3\\\3\2\2\2\13\2\u0177\u0179\u017f\u0188\u018a\u0198\u01a5"+
		"\u01b0\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}