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

	/**
	 * Helper method, reverse the direction of the operator when the lhs and rhs are swapped.
	 * Note this method is not reverse the meaning of the operator. 
	 * For example, if the operator is "<", this method will return ">", but NOT ">=".
	 * A typical use case is, "a>b" -> "b>a". Operator ">" is changed to Operator "<".
	 */
	public Operator getReverseOperator() {
		switch (this.opr) {
		case "<":
			return new Operator(">");
		case "<=":
			return new Operator(">=");
		case ">=":
			return new Operator("<=");
		case ">":
			return new Operator("<");
		// for "=", "!=", "<>", we just return the same operator.
		default:
			return this;
		}
	}
	@Override
	public String toString() {
		return this.opr;
	}
}
