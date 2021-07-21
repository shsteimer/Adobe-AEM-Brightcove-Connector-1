package com.coresecure.brightcove.wrapper.services;

import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.day.cq.replication.ReplicationActionType;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;

/**
 * A handler for replicating types of assets.
 * Default implementations are for videos and playlists, but others could be added in the future if needed.
 */
public interface BrightcoveAssetReplicationHandler {

    /**
     * Determine if the asset can be handled by this handler.
     *
     * @param assetResource the asset resource
     * @return true if this handler can handle the asset
     */
    boolean isHandled(Resource assetResource);

    /**
     * Handles actually replication the particular asset to brightcove.
     *
     * @param resourceResolver      the resource resolver
     * @param assetResource         the asset resource
     * @param replicationActionType the replication type
     * @param bcAccountService      the brightcove account service
     */
    void handle(ResourceResolver resourceResolver, Resource assetResource, ReplicationActionType replicationActionType, ConfigurationService bcAccountService) throws BrightcoveReplicationException;
}
