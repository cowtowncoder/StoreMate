package com.fasterxml.storemate.api;

/**
 * Builder object used for constructing {@link RequestPath}
 * instances.
 */
public abstract class RequestPathBuilder
{
	public abstract RequestPathBuilder addPathSegment(String segment);
	
	public abstract RequestPathBuilder addParameter(String key, String value);
	
	public abstract RequestPath build();
}
