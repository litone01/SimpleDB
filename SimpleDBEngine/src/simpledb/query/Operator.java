package simpledb.query;

import simpledb.record.Schema;

import simpledb.parse.BadSyntaxException;

public class Operator {
	private String opr;
	public Operator(String s) {
		this.opr = s;
	}
	
	public boolean check(Constant lhs, Constant rhs) {
		int compareResult = lhs.compareTo(rhs);
		switch (this.opr) {
		case "<":
			return compareResult < 0;
		case "<=":
			return compareResult <= 0;
		case "=":
			return compareResult == 0;
		case ">=":
			return compareResult >= 0;
		case ">":
			return compareResult > 0;
		case "!=":
			return compareResult != 0;
		case "<>":
			return compareResult != 0;
		default:
			return false;
		}
		
	}
}
