package com.fasterxml.storemate.json;

import com.fasterxml.storemate.api.ClusterStatusResponse;
import com.fasterxml.storemate.api.NodeState;
import com.fasterxml.storemate.json.StoreMateObjectMapper;

/**
 * Simple unit tests to verify that we can map basic JSON data
 * as expected. This verifies both basic definitions of types
 * we care about and correct functioning of mr Bean module
 * used to materialize abstract types.
 */
public class TestObjectMapper extends JsonTestBase
{
    public void testNodeState() throws Exception
    {
        StoreMateObjectMapper mapper = new StoreMateObjectMapper();
        NodeState state = mapper.readValue("{}", NodeState.class);
        assertNotNull(state);
    }

    public void testClusterStatusResponse() throws Exception
    {
        StoreMateObjectMapper mapper = new StoreMateObjectMapper();
        ClusterStatusResponse resp = mapper.readValue("{\"local\":{}}",
                ClusterStatusResponse.class);
        assertNotNull(resp);
        assertNotNull(resp.local);
    }
}
