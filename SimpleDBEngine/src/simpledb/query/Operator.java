package simpledb.query;

/**
 * A class that stores the type of an operator. 
 * This is added to support non-equality operator for lab 1.
 */
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

	@Override
	public String toString() {
		return this.opr;
	}
}
