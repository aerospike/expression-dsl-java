package com.aerospike.dsl;

/**
 * This class stores values to be matched with placeholders by indexes
 */
public class PlaceholderValues {

    private final Object[] values;

    /**
     * Create new {@link PlaceholderValues} object
     *
     * @param values Values matching placeholders in DSL input string
     */
    public PlaceholderValues(Object... values) {
        this.values = values != null ? values : new Object[0];
    }

    /**
     * Get value of the placeholder with the particular index
     *
     * @param index Index of the placeholder
     * @return Value of the placeholder with the given index
     * @throws IllegalArgumentException if placeholder index is out of bounds (fewer values than placeholders)
     */
    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            throw new IllegalArgumentException("Missing value for placeholder ?" + index);
        }
        return values[index];
    }

    /**
     * Get overall amount of the given values
     */
    public int size() {
        return values.length;
    }
}
