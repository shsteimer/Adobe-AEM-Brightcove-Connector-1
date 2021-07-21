package com.coresecure.brightcove.wrapper.services.impl;

import com.adobe.cq.dam.cfm.ContentFragment;
import com.coresecure.brightcove.wrapper.BrightcoveAPI;
import com.coresecure.brightcove.wrapper.enums.PlaylistTypeEnum;
import com.coresecure.brightcove.wrapper.objects.Playlist;
import com.coresecure.brightcove.wrapper.objects.Video;
import com.coresecure.brightcove.wrapper.services.BrightcoveAssetReplicationHandler;
import com.coresecure.brightcove.wrapper.services.BrightcoveReplicationException;
import com.coresecure.brightcove.wrapper.services.BrightcoveVideoFilter;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.coresecure.brightcove.wrapper.sling.ServiceUtil;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.coresecure.brightcove.wrapper.utils.JcrUtil;
import com.day.cq.commons.jcr.JcrConstants;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.dam.commons.util.DamUtil;
import com.day.cq.replication.ReplicationActionType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.day.cq.replication.ReplicationActionType.ACTIVATE;
import static com.day.cq.replication.ReplicationActionType.DEACTIVATE;

@Component(service = BrightcoveAssetReplicationHandler.class)
@Designate(ocd = BrightcovePlaylistReplicationHandlerImpl.Config.class)
public class BrightcovePlaylistReplicationHandlerImpl implements BrightcoveAssetReplicationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BrightcovePlaylistReplicationHandlerImpl.class);

    private Config config;

    @Override
    public boolean isHandled(Resource assetResource) {
        ContentFragment fragment = assetResource.adaptTo(ContentFragment.class);
        Resource assetDataResource = assetResource.getChild(
                JcrConstants.JCR_CONTENT + "/" + Constants.NN_CF_JCR_CONTENT_DATA);
        if (null != assetDataResource && null != fragment) {
            String cfModel = assetDataResource.getValueMap().get(Constants.PN_CF_CQ_MODEL, StringUtils.EMPTY);
            return cfModel.equals(config.cf_model_playlist());
        }
        return false;
    }

    @Override
    public void handle(ResourceResolver resourceResolver, Resource assetResource, ReplicationActionType replicationActionType, ConfigurationService bcAccountService) throws BrightcoveReplicationException {
        BrightcoveAPI brAPI = new BrightcoveAPI(bcAccountService.getAccountID());

        handleInternal(brAPI, resourceResolver, assetResource, replicationActionType);
    }

    void handleInternal(BrightcoveAPI brAPI, ResourceResolver resourceResolver, Resource assetResource,
                        ReplicationActionType replicationActionType) throws BrightcoveReplicationException {
        try {

            if (assetResource != null && brAPI != null && replicationActionType != null) {
                Asset asset = assetResource.adaptTo(Asset.class);
                if (null != asset) {
                    Resource metadataRes = assetResource.getChild(Constants.ASSET_METADATA_PATH);
                    if (metadataRes != null) {
                        ModifiableValueMap metadataVm = metadataRes.adaptTo(ModifiableValueMap.class);
                        executePlaylistRequest(replicationActionType, asset, assetResource, metadataVm, brAPI, resourceResolver);

                        if (resourceResolver.hasChanges()) {
                            resourceResolver.commit();
                        }
                    }
                }

            }
        } catch (PersistenceException e) {
            throw new BrightcoveReplicationException("sync to brightcove failed.", e);
        }
    }

    private void executePlaylistRequest(ReplicationActionType replicationActionType, Asset asset,
                                        Resource assetResource, ModifiableValueMap metadataVm, BrightcoveAPI brAPI,
                                        ResourceResolver resourceResolver) throws BrightcoveReplicationException {

        String staticPlaylistId = asset.getMetadataValue(Constants.PN_BC_METADATA_STATIC_PLAYLIST);

        ContentFragment contentFragment = assetResource.adaptTo(ContentFragment.class);

        if (null != contentFragment) {

            String playlistName = getElementStringValue(contentFragment, Constants.PN_BC_PLAYLIST_NAME);
            String playlistDescription = getElementStringValue(contentFragment,
                    Constants.PN_BC_PLAYLIST_DESCRIPTION);
            String[] playlistVideoIds = getPlaylistIds(contentFragment);

            if (replicationActionType.equals(DEACTIVATE) && StringUtils.isNotBlank(staticPlaylistId)) {
                deletePlaylist(brAPI, staticPlaylistId, metadataVm);
            } else if (replicationActionType.equals(ACTIVATE) && StringUtils.isNotBlank(playlistName)
                    && ArrayUtils.isNotEmpty(playlistVideoIds)) {
                List<String> ids = validatePlaylistIds(resourceResolver, playlistVideoIds);
                if (StringUtils.isNotBlank(staticPlaylistId)) {
                    updatePlaylist(staticPlaylistId, ids.toArray(new String[0]), brAPI, metadataVm);
                } else {
                    createPlaylist(asset, playlistName, playlistDescription, ids, brAPI, metadataVm);
                }
            }
        }

    }

    private void updatePlaylist(String staticPlaylistId, String[] playlistVideoIds, BrightcoveAPI brAPI, ModifiableValueMap metadataVm)
            throws BrightcoveReplicationException {
        JSONObject result = brAPI.cms.updatePlaylist(staticPlaylistId, playlistVideoIds);
        if (!result.has(Constants.ID) && result.length() > 0) {
            throw new BrightcoveReplicationException("Updating playlist failed to send. json: " + result);
        }
    }

    private void createPlaylist(Asset asset, String playlistName, String playlistDescription,
                                List<String> playlistVideoIds, BrightcoveAPI brAPI, ModifiableValueMap metadataVm)
            throws BrightcoveReplicationException {
        try {
            Playlist playlist = new Playlist();
            playlist.setName(playlistName);
            playlist.setDescription(playlistDescription);
            playlist.setPlaylistType(PlaylistTypeEnum.EXPLICIT);
            playlist.setReferenceId(asset.getID());
            playlist.setVideoIds(playlistVideoIds);
            JSONObject result = brAPI.cms.createPlaylist(playlist);
            if (result != null && result.has(Constants.ID)) {
                LOG.debug("New Playlist ID: {}", result.getString("id"));
                metadataVm.put(Constants.PN_BC_METADATA_STATIC_PLAYLIST, result.getString("id"));
            } else {
                throw new BrightcoveReplicationException(
                        "Creating playlist failed to send. json: " + result);
            }
        } catch (JSONException e) {
            throw new BrightcoveReplicationException("creating playlist failed to send. json: ", e);
        }
    }

    private void deletePlaylist(BrightcoveAPI brAPI, String staticPlaylistId, ModifiableValueMap metadataVm) {
        brAPI.cms.deletePlaylist(staticPlaylistId);
        metadataVm.put(Constants.PN_BC_METADATA_STATIC_PLAYLIST, StringUtils.EMPTY);
    }

    private List<String> validatePlaylistIds(ResourceResolver resourceResolver, String[] playlistVideoIds) {

        //the ids in the playlist cf can be either the path to a brightcove synced video asset, or just the raw brightcove id
        //this method ensures both are handled

        Set<String> videoIDs = new LinkedHashSet<>();
        for (String idStr : playlistVideoIds) {
            if (StringUtils.startsWith(idStr, config.path_dam_root())) {
                validateDAMPlaylistIds(resourceResolver, idStr, videoIDs);
            } else {
                videoIDs.add(idStr);
            }
        }
        return videoIDs.stream().collect(Collectors.toList());
    }

    private void validateDAMPlaylistIds(ResourceResolver resourceResolver, String path, Set<String> videoIDs) {
        Resource damResource = resourceResolver.getResource(path);
        if (null != damResource) {
            Resource metadataResource = damResource
                    .getChild(JcrConstants.JCR_CONTENT + "/" + DamConstants.METADATA_FOLDER);
            if (null != metadataResource) {
                ValueMap metadataValueMap = metadataResource.getValueMap();
                String brightcoveId = metadataValueMap.get(Constants.BRC_ID, StringUtils.EMPTY);
                if (StringUtils.isNotBlank(brightcoveId)) {
                    videoIDs.add(brightcoveId);
                }
            }
        }
    }

    private String[] getPlaylistIds(final ContentFragment contentFragment) {
        return (contentFragment.hasElement(Constants.PN_BC_PLAYLIST_VIDEOIDS) && null != contentFragment
                .getElement(Constants.PN_BC_PLAYLIST_VIDEOIDS)) ? contentFragment
                .getElement(Constants.PN_BC_PLAYLIST_VIDEOIDS)
                .getValue().getValue(String[].class) : new String[0];
    }

    private String getElementStringValue(final ContentFragment contentFragment, final String property) {
        return (contentFragment.hasElement(property) && null != contentFragment.getElement(property))
                ? contentFragment.getElement(property).getValue().getValue(String.class)
                : StringUtils.EMPTY;
    }

    @Activate
    public void activate(Config config) {
        this.config = config;
    }

    @ObjectClassDefinition(name = "Brightcove Playlist")
    public @interface Config {

        @AttributeDefinition(name = "Brightcove Playlist Content Fragment Model")
        String cf_model_playlist() default "/conf/brightcove/settings/dam/cfm/models/playlist-content-fragment-model";

        @AttributeDefinition(name = "Playlists Root Path")
        String path_dam_root() default "/content/dam";
    }

}
