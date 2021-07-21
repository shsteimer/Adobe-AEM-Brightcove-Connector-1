package com.coresecure.brightcove.wrapper.config;

import com.coresecure.brightcove.wrapper.sling.ConfigurationGrabber;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.caconfig.ConfigurationBuilder;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = BrightcoveAccountService.class)
public class BrightcoveAccountService {

    @Reference
    private ConfigurationGrabber configurationGrabber;

    public ConfigurationService getService(Resource resource) {
        String accountId = null;
        if (resource != null) {
            BrightcoveAccountConfiguration bcAccountConf = resource.adaptTo(ConfigurationBuilder.class).as(BrightcoveAccountConfiguration.class);
            accountId = bcAccountConf.accountId();

            //if no value in ca config, use the old method of using the name of the parent folder
            if (StringUtils.isBlank(accountId)) {
                accountId = resource.getParent() != null ? resource.getParent().getName() : null;
            }
        }

        if (StringUtils.isNotBlank(accountId)) {
            return configurationGrabber.getConfigurationService(accountId);
        }

        return null;
    }
}
