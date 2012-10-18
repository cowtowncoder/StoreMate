package com.fasterxml.storemate.api;

/**
 * Immutable class that defines generic API for paths used to make
 * calls using network clients. Network client implementations create
 * {@link RequestPathBuilder}s to use, and accept
 * {@link RequestPath} instances as call targets.
 */
public abstract class RequestPath
{
	/**
	 * Factory method for creating builder instance that can
	 * be used for building a more refined path, given this
	 * instance as the base.
	 */
	public abstract RequestPathBuilder builder();
}
