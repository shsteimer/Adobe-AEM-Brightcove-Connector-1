package com.coresecure.brightcove.wrapper.listeners;

import com.coresecure.brightcove.wrapper.config.BrightcoveAccountService;
import com.coresecure.brightcove.wrapper.services.BrightcoveAssetReplicationHandler;
import com.coresecure.brightcove.wrapper.services.BrightcoveReplicationException;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.day.cq.replication.ReplicationAction;
import com.day.cq.replication.ReplicationActionType;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component(service = {EventHandler.class, JobConsumer.class},
        immediate = true,
        property = {
                "service.description" + "=Brightcove Distribution Event Listener ",
                EventConstants.EVENT_TOPIC + "=" + ReplicationAction.EVENT_TOPIC,
                JobConsumer.PROPERTY_TOPICS + "=" + BrightcovePublishListener.JOB_TOPIC
        }
)
@Designate(ocd = BrightcovePublishListener.EventListenerPageActivationListenerConfiguration.class)
public class BrightcovePublishListener implements EventHandler, JobConsumer {

    @ObjectClassDefinition(name = "Brightcove Distribution Listener")
    public @interface EventListenerPageActivationListenerConfiguration {

        @AttributeDefinition(
                name = "Enabled",
                description = "Enable Distribution Event Listener",
                type = AttributeType.BOOLEAN
        )
        boolean isEnabled() default false;
    }

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private JobManager jobManager;

    @Reference
    private BrightcoveAccountService brightcoveAccountService;

    @Reference
    private transient List<BrightcoveAssetReplicationHandler> handlers;

    @Reference
    SlingSettingsService slingSettings;


    private static final Logger LOG = LoggerFactory.getLogger(BrightcovePublishListener.class);
    public static final String JOB_TOPIC = "brightcove/assets/replication";

    private boolean enabled = false;
    private Map<String, String> paths = null;

    @Activate
    @Modified
    protected void activate(EventListenerPageActivationListenerConfiguration config) {
        enabled = config.isEnabled();
        LOG.info("Brightcove Distribution Event Listener is enabled: " + enabled);
    }

    @Override
    public JobResult process(Job job) {

        // grab a resource resolver to pass to all the activation methods
        final Map<String, Object> authInfo = Collections.singletonMap(
                ResourceResolverFactory.SUBSERVICE,
                Constants.SERVICE_ACCOUNT_IDENTIFIER);
        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(authInfo)) {
            String assetPath = job.getProperty("assetPath", "");
            String replicationType = job.getProperty("replicationType", "");
            ReplicationActionType replicationActionType = ReplicationActionType.fromName(replicationType);

            Resource assetResource = resolver.getResource(assetPath);
            if (assetResource != null) {
                ConfigurationService service = brightcoveAccountService.getService(assetResource);
                if (service != null) {

                    Optional<BrightcoveAssetReplicationHandler> handler = handlers.stream().filter(h -> h.isHandled(assetResource)).findFirst();
                    if (handler.isPresent()) {
                        handler.get().handle(resolver, assetResource, replicationActionType, service);
                    }
                }
            }
        } catch (LoginException e) {
            LOG.error("There was an error using the Brightcove system user.", e);
            return JobResult.FAILED;
        } catch (BrightcoveReplicationException e) {
            LOG.error("There was an error using while replicating with handler.", e);
            return JobResult.FAILED;
        }

        return JobResult.OK;
    }

    @Override
    public void handleEvent(Event event) {

        // check that the service is enabled and that we are running on Author
        if (enabled && slingSettings.getRunModes().contains("author")) {

            //event handlers must be fast, so fire a job for each path and do the processing there
            ReplicationAction replicationAction = ReplicationAction.fromEvent(event);

            ReplicationActionType type = replicationAction.getType();
            String[] paths = replicationAction.getPaths();

            for (String path : paths) {
                final Map<String, Object> props = new HashMap<>();
                props.put("assetPath", path);
                props.put("replicationType", type.getName());

                Job job = jobManager.addJob(JOB_TOPIC, props);
                LOG.info("job {} created to publish {} to brightcove - type {}", job.getId(), path, type.getName());
            }

        }

    }
    
}