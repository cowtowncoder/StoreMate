package com.fasterxml.storemate.shared;

import java.util.ArrayList;
import java.util.List;

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
     * Convenience method for appending a sequence of path segments,
     * as if calling {@link #addPathSegment(String)} once per segment,
     * in the order indicated.
     */
    public RequestPathBuilder addPathSegments(String[] segments)
    {
        RequestPathBuilder builder = this;
        for (String segment : segments) {
            builder = builder.addPathSegment(segment);
        }
        return builder;
    }
    
    /**
     * Method that will add a single query parameter in the logical
     * path, with given value.
     *
     * @return Builder instance to use for further calls (may be 'this',
     *   but does not have to be)
     */
    public abstract RequestPathBuilder addParameter(String key, String value);

    /**
     * Method for returning only the logical "server part", which also includes
     * the protocol (like 'http') and port number, as well as trailing
     * slash.
     */
    public abstract String getServerPart();

    /**
     * Method for returning only the logical "path" part, without including
     * either server part or query parameters.
     */
    public abstract String getPath();
    
    /**
     * Method that will construct the immutable {@link RequestPath} instance
     * with information builder has accumulated.
     */
    public abstract RequestPath build();

    /**
     * Implementations MUST override this to produce a valid URL that
     * represents the current state of builder.
     */
    @Override
    public abstract String toString();

    /*
    /*********************************************************************
    /* Helper methods for sub-classes
    /*********************************************************************
     */

    protected static List<String> _arrayToList(String[] qp)
    {
         if (qp == null) {
              return new ArrayList<String>(8);
         }
         int len = qp.length;
         List<String> list = new ArrayList<String>(Math.min(8, len));
         if (len > 0) {
              for (int i = 0; i < len; ++i) {
                   list.add(qp[i]);
              }
         }
         return list;
    }
}
