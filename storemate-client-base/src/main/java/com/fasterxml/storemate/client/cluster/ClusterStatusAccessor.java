package com.fasterxml.storemate.client.cluster;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.api.ClusterStatusResponse;
import com.fasterxml.storemate.api.HTTPConstants;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.util.IOUtil;

/**
 * Helper class that handles details of getting cluster status information
 * from a store node.
 */
public class ClusterStatusAccessor extends Loggable
{
    protected final static long MIN_TIMEOUT_MSECS = 10L;
    
    protected final ObjectMapper _mapper;
    
    public ClusterStatusAccessor(ObjectMapper m) {
        super(ClusterStatusAccessor.class);
        _mapper = m;
    }
    
    public ClusterStatusResponse getClusterStatus(IpAndPort ip, long timeoutMsecs)
        throws IOException
    {
        // first: if we can't spend at least 10 msecs, let's give up:
        if (timeoutMsecs < MIN_TIMEOUT_MSECS) {
            return null;
        }
        
        HttpURLConnection conn;
        String endpoint = ip.getEndpoint() + HTTPConstants.PATH_CLUSTER_STATUS;

        try {
            URL url = new URL(endpoint);
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            throw new IOException("Can not access Cluster state using '"+endpoint+"': "+e.getMessage());
        }
        conn.setRequestMethod("GET");
        // not optimal, will have to do:
        conn.setConnectTimeout((int) timeoutMsecs);
        conn.setReadTimeout((int) timeoutMsecs);
        int status = conn.getResponseCode();
        if (!IOUtil.isHTTPSuccess(status)) {
            // should we read the error message?
            throw new IOException("Failed to access Cluster state using '"+endpoint+"': response code "
                    +status);
        }
        
        InputStream in;
        try {
            in = conn.getInputStream();
        } catch (IOException e) {
            throw new IOException("Can not access Cluster state using '"+endpoint+"': "+e.getMessage());
        }
        ClusterStatusResponse result;
        try {
            result = _mapper.readValue(in, ClusterStatusResponse.class);
        } catch (IOException e) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', failed to parse JSON: "+e.getMessage());
        } finally {
            try {
                in.close();
            } catch (IOException e) { }
        }
        // validate briefly, just in case:
        if (result.local == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'local' info");
        }
        if (result.local.getAddress() == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'local.address' info");
        }
        if (result.remote == null) {
            throw new IOException("Invalid Cluster state returned by '"+endpoint+"', missing 'remote' info"); 
        }
        return result;
    }
}
