package simpledb.query;

import java.util.Map;

// Enum class is inspired based on Joshua Bloch's Effective Java
public enum OrderByType {
    ASC("asc"), 
    DESC("desc");

    private final String order;
    private static final Map<String, OrderByType> ORDERBYTYPE_MAP;

    OrderByType(String order) {
        this.order = order;
    }

    // Instance method to retrieve the String value associated with the enum object
    public String order() {
        return order;
    }

    // Build an immutable map of String to enum pairs
    static {
        Map<String, OrderByType> map = new java.util.HashMap<String, OrderByType>();
        for (OrderByType type : OrderByType.values()) {
            map.put(type.order, type);
        }
        ORDERBYTYPE_MAP = java.util.Collections.unmodifiableMap(map);
    }

    public static OrderByType getOrderByType(String order) {
        return ORDERBYTYPE_MAP.get(order);
    }
}
