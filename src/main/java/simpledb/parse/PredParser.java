package simpledb.parse;

public class PredParser {
    private Lexer lex;

    private StringBuilder parseTree;

    public PredParser(String s) {
        lex = new Lexer(s);
        parseTree = new StringBuilder();
    }

    public String field() {
        String id = lex.eatId();
        String treeNode = "/" +
                id;
        parseTree.append(treeNode);
        return lex.eatId();
    }

    public void constant() {
        String treeNode = "/\n";
        if (lex.matchStringConstant()) {
            treeNode += lex.eatStringConstant();
        } else {
            treeNode += lex.eatIntConstant();
        }
        parseTree.append(treeNode);
    }

    public void expression() {
        String treeNode = "/\n";
        if (lex.matchId()) {
            treeNode += field();
            parseTree.append(treeNode);
        } else
            constant();
    }

    public void term() {
        String treeNode = "exp\n";
        expression();
        lex.eatDelim('=');
        expression();
        parseTree.append(treeNode);
    }

    public void predicate() {
        String treeNode = "pred\n";
        term();
        parseTree.append(treeNode);
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            predicate();
        }
    }
}

