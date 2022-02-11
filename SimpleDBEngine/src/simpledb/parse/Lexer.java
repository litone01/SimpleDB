package simpledb.parse;

import java.util.*;
import java.io.*;

import simpledb.query.Operator;
import simpledb.query.OrderByType;;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   // add operators list for supporting non-equality operations
   private Collection<String> operators;
   private Collection<Character> singleOprs;
   // add order by type list for supporting order by clause
   private Collection<String> orderByTypes;
   private StreamTokenizer tok;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initOperators();
      initOrderByTypes();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.lowerCaseMode(true); //ids and keywords are converted
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
   }

   public boolean matchIndexType() {
      return  tok.ttype==StreamTokenizer.TT_WORD && keywords.contains(tok.sval) 
            && (tok.sval.equals("hash") || tok.sval.equals("btree"));
   }
   
   /**
    * Returns true if the current token is one of the legal single operators.
    * @return true if the current token is one of the legal single operators.
    */
   public boolean matchSingleOperator() {
	   return singleOprs.contains((char) tok.ttype);
   }
   
   public boolean matchOrderByType() {
      return tok.ttype==StreamTokenizer.TT_WORD && orderByTypes.contains(tok.sval);
   }

//Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not a legal operator. 
    * Otherwise, moves to the next token.
    * @return the corresponding Operator object of the current token
    */
   public Operator eatOpr() {
      String tokSval = "";
	   // 1. match the first single operator
      if (!matchSingleOperator()) {
         throw new BadSyntaxException();
      }
      tokSval += (char) tok.ttype;
      // 2. try to match the second single operator
      nextToken();
      if (matchSingleOperator()) {
         tokSval += (char) tok.ttype;
         nextToken();
      }
      // 3. check if the combined operator is legal
      if (!operators.contains(tokSval)) {
         throw new BadSyntaxException();
      }
      // 4. return the operator
      Operator opr = new Operator(tokSval);
      return opr;
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an identifier. 
    * Otherwise, returns the identifier string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not 
    * an index type. 
    * Otherwise, returns the index type string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatIndexType() {
      if (!matchIndexType())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a legal order by type, that is either "asc" or "desc". 
    * Otherwise, returns the orderByType enum object
    * and moves to the next token.
    * @return the corresponding orderByType enum object
    */
   public OrderByType eatOrderByType() {
      if (!matchOrderByType())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return OrderByType.getOrderByType(s);
   }


   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }
   
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index", "on",
                               "using", "hash", "btree");
   }
   
   // init operators and singleOprs for matching and eating in the Lexer
   private void initOperators() {
      operators = Arrays.asList("<", "<=", "=", ">=", ">", "!=", "<>");
      singleOprs = Arrays.asList('<', '>', '=', '!');
	}

    // init orderByTypes for matching and eating in the Lexer
    private void initOrderByTypes() {
      orderByTypes = Arrays.asList("asc", "desc");
	}
}