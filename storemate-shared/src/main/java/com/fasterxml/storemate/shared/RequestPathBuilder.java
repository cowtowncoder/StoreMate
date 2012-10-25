package com.fasterxml.storemate.shared;

/**
 * Builder object used for constructing {@link RequestPath}
 * instances.
 */
public abstract class RequestPathBuilder
{
    /**
     * Method that will append a single path segment, escaping characters
     * as necessary.
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract RequestPathBuilder addPathSegment(String segment);
	
    /**
     * Method that will add a single query parameter in the logical
     * path, with given value.
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract RequestPathBuilder addParameter(String key, String value);

    /**
     * Method that will construct the immutable {@link RequestPath} instance
     * with information builder has accumulated.
     */
    public abstract RequestPath build();
}
