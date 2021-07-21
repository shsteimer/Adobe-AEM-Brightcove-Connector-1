package com.coresecure.brightcove.wrapper.config;

import org.apache.sling.caconfig.annotation.Configuration;

@Configuration(label = "Brightcove Account Configuration")
public @interface BrightcoveAccountConfiguration {

    /**
     * The brightcove account id to use. This id must be match the account id for a
     * brightcove account configured with com.coresecure.brightcove.wrapper.sling.BrcServiceImpl
     * @return the account id.
     */
    String accountId();
}
