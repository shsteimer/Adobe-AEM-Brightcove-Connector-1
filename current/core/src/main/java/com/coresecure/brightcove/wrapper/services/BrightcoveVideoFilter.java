package com.coresecure.brightcove.wrapper.services;

import com.day.cq.dam.api.Asset;
import org.osgi.annotation.versioning.ConsumerType;

@ConsumerType
public interface BrightcoveVideoFilter {

    /**
     * Determines if a specified asset should be synchronized to brightcove.
     *
     * This allows for application specific logic to determine which videos are sent to brightcove
     * (for example, based on metadata).
     *
     * @param asset the asset
     * @return true if the video should be synchronized to brightcove, false otherwise
     */
    boolean isAllowed(Asset asset);
}
