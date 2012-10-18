package com.fasterxml.storemate.api;

/**
 * Type that defines how references are built to access
 * a StoreMate service node.
 */
public abstract class RequestPathStrategy
{
	/**
	 * Method for creating the path for accessing stored entries,
	 * but without including actual entry id, given a builder that
	 * refers to the server node to access
	 * 
	 * @param storeRoot Reference to root of the logical store reference
	 * 
	 * @return Path for accessing stored entries, not including the actual
	 *    entry id.
	 */
	public abstract RequestPathBuilder appendStoreEntryPath(RequestPathBuilder storeRoot);
}
