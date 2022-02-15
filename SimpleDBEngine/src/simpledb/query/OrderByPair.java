package simpledb.query;

/**
 * Pair of a field name and its order for the Order By Clause in a SQL SELECT query.
 */
public class OrderByPair {
    private final String field;
    private final OrderByType order;

    public OrderByPair(String field, OrderByType order) {
        this.field = field;
        this.order = order;
    }

    public String field() {
        return field;
    }

    public OrderByType order() {
        return order;
    }

    @Override
    public String toString() {
        return field + " " + order;
    }
}
