package com.coresecure.brightcove.wrapper.services.impl;

import com.coresecure.brightcove.wrapper.objects.Video;
import com.coresecure.brightcove.wrapper.services.BrightcoveAssetReplicationHandler;
import com.coresecure.brightcove.wrapper.services.BrightcoveReplicationException;
import com.coresecure.brightcove.wrapper.services.BrightcoveVideoFilter;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.coresecure.brightcove.wrapper.sling.ServiceUtil;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.coresecure.brightcove.wrapper.utils.JcrUtil;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.dam.commons.util.DamUtil;
import com.day.cq.replication.ReplicationActionType;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.json.JSONObject;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Date;

@Component(service = BrightcoveAssetReplicationHandler.class)
public class BrightcoveVideoReplicationHandlerImpl implements BrightcoveAssetReplicationHandler {

    @Reference(cardinality = ReferenceCardinality.OPTIONAL)
    private BrightcoveVideoFilter videoFilter;

    private static final Logger LOG = LoggerFactory.getLogger(BrightcoveVideoReplicationHandlerImpl.class);

    @Override
    public boolean isHandled(Resource assetResource) {
        Asset asset = assetResource.adaptTo(Asset.class);
        if (asset != null) {
            boolean isVideo = !DamUtil.isSubAsset(assetResource) && DamUtil.isVideo(asset);
            if (isVideo) {
                // this bit allows for application specific logic to filter the videos that are synchronized without having the re-implement this whole service
                // for example, requiring certain metadata for the video to be synchronized
                if (videoFilter != null) {
                    return videoFilter.isAllowed(asset);
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public void handle(ResourceResolver resourceResolver, Resource assetResource, ReplicationActionType replicationActionType, ConfigurationService bcAccountService) throws BrightcoveReplicationException {
        if (replicationActionType.equals(ReplicationActionType.ACTIVATE)) {
            activateAsset(resourceResolver, assetResource.adaptTo(Asset.class), bcAccountService.getAccountID());
        } else if (replicationActionType.equals(ReplicationActionType.DEACTIVATE)) {
            deactivateAsset(resourceResolver, assetResource.adaptTo(Asset.class), bcAccountService.getAccountID());
        }

        if(resourceResolver.hasChanges()) {
            try {
                resourceResolver.commit();
            } catch (PersistenceException e) {
                throw new BrightcoveReplicationException("failed to persist changes for video.", e);
            }
        }
    }

    private void activateNew(Asset _asset, ServiceUtil serviceUtil, Video video, ModifiableValueMap brc_lastsync_map) {

        LOG.trace("brc_lastsync was null or zero : asset should be initialized");

        try {

            // get the binary
            InputStream is = _asset.getOriginal().getStream();

            // make the actual video upload call
            JSONObject api_resp = serviceUtil.createVideoS3(video, _asset.getName(), is);

            //LOGGER.trace("API-RESP >>" + api_resp.toString(1));
            boolean sent = api_resp.getBoolean(Constants.SENT);
            if (sent) {

                brc_lastsync_map.put(Constants.BRC_ID, api_resp.getString(Constants.VIDEOID));

                LOG.trace("UPDATING RENDITIONS FOR THIS ASSET");
                serviceUtil.updateRenditions(_asset, video);


                LOG.info("BC: ACTIVATION SUCCESSFUL >> {}", _asset.getPath());

                // update the metadata to show the last sync time
                brc_lastsync_map.put(DamConstants.DC_TITLE, video.name);
                brc_lastsync_map.put(Constants.BRC_LASTSYNC, JcrUtil.now2calendar());

            } else {

                // log the error
                LOG.error(Constants.REP_ACTIVATION_SUCCESS_TMPL, _asset.getName());

            }

        } catch (Exception e) {

            LOG.error("Error: {}", e.getMessage());

        }

    }

    private void activateModified(Asset _asset, ServiceUtil serviceUtil, Video video, ModifiableValueMap brc_lastsync_map) {

        LOG.info("Entering activateModified()");

        try {

            // do update video
            LOG.info("About to make Brightcove API call with video: {}", _asset.getPath());
            JSONObject api_resp = serviceUtil.updateVideo(video);
            LOG.info("Brightcove Asset Modification Response: {}", api_resp.toString());

            boolean sent = api_resp.getBoolean(Constants.SENT);
            if (sent) {

                LOG.info("Brightcove video updated successfully: {}", _asset.getPath());
                serviceUtil.updateRenditions(_asset, video);
                LOG.info("Updated renditions for Brightcove video: {}", _asset.getPath());

                long current_time_millisec = new Date().getTime();
                brc_lastsync_map.put(Constants.BRC_LASTSYNC, current_time_millisec);

            } else {

                // log the error
                LOG.error("Error sending data to Brightcove: {}", _asset.getName());

            }
        } catch (Exception e) {

            // log the error
            LOG.error("General Error: {}", _asset.getName());

        }

    }

    private void activateAsset(ResourceResolver rr, Asset _asset, String accountId) {

        // need to either activate a new asset or an updated existing
        ServiceUtil serviceUtil = new ServiceUtil(accountId);
        String path = _asset.getPath();

        Video video = serviceUtil.createVideo(path, _asset, "ACTIVE");
        Resource assetRes = _asset.adaptTo(Resource.class);

        if (assetRes == null) {
            return;
        }

        Resource metadataRes = assetRes.getChild(Constants.ASSET_METADATA_PATH);
        if (metadataRes == null) {
            return;
        }

        ModifiableValueMap brc_lastsync_map = metadataRes.adaptTo(ModifiableValueMap.class);
        if (brc_lastsync_map == null) {
            return;
        }

        Long jcr_lastmod = _asset.getLastModified();
        Long brc_lastsync_time = brc_lastsync_map.get(Constants.BRC_LASTSYNC, Long.class);

        brc_lastsync_map.put(Constants.BRC_STATE, "ACTIVE");

        if (brc_lastsync_time == null) {

            // we need to activate a new asset here
            LOG.info("Activating New Brightcove Asset: {}", _asset.getPath());
            activateNew(_asset, serviceUtil, video, brc_lastsync_map);

        } else if (jcr_lastmod > brc_lastsync_time) {

            // we need to modify an existing asset here
            LOG.info("Activating Modified Brightcove Asset: {}", _asset.getPath());
            activateModified(_asset, serviceUtil, video, brc_lastsync_map);

        }

    }

    private void deactivateAsset(ResourceResolver rr, Asset _asset, String accountId) {

        // need to deactivate (delete) an existing asset
        Resource assetRes = _asset.adaptTo(Resource.class);
        ServiceUtil serviceUtil = new ServiceUtil(accountId);

        if (assetRes != null) {

            Resource metadataRes = assetRes.getChild(Constants.ASSET_METADATA_PATH);
            if (metadataRes != null) {

                ModifiableValueMap brc_lastsync_map = metadataRes.adaptTo(ModifiableValueMap.class);

                if (brc_lastsync_map != null) {

                    brc_lastsync_map.put(Constants.BRC_STATE, "INACTIVE");

                    LOG.info("Deactivating New Brightcove Asset: {}", _asset.getPath());
                    String path = _asset.getPath();

                    Video video = serviceUtil.createVideo(path, _asset, "INACTIVE");

                    LOG.trace("Sending Brightcove Video to API Call: {}", video.toString());

                    try {

                        JSONObject update_resp = serviceUtil.updateVideo(video);
                        boolean sent = update_resp.getBoolean(Constants.SENT);

                        if (sent) {

                            LOG.info("Brightcove update successful for: {}", _asset.getPath());

                        } else {

                            LOG.error("Brightcove update unsuccessful for: {}", _asset.getName());

                        }

                    } catch (Exception e) {

                        LOG.error("Error!: {}", e.getMessage());

                    }
                }
            }
        }

    }
}
