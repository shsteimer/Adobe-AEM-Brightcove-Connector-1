package com.coresecure.brightcove.wrapper.sync;

import com.coresecure.brightcove.wrapper.sling.ServiceUtil;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.day.cq.dam.api.Asset;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

@Component(service = JobConsumer.class, immediate = true, property = {
        JobConsumer.PROPERTY_TOPICS + "=" + BrightcoveSynchronizationVideoUpdateJob.JOB_TOPIC
})
public class BrightcoveSynchronizationVideoUpdateJob implements JobConsumer {

    public static final String JOB_TOPIC = BrightcoveSynchronizationScheduledJob.JOB_TOPIC + "/update";

    private static final Logger log = LoggerFactory.getLogger(BrightcoveSynchronizationVideoUpdateJob.class);

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Override
    @SuppressWarnings("squid:CallToDeprecatedMethod")
    public JobResult process(Job job) {

        Map<String, Object> bcServiceParams = Collections.singletonMap(ResourceResolverFactory.SUBSERVICE,
                                        Constants.SERVICE_ACCOUNT_IDENTIFIER);
        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(bcServiceParams)) {
            String assetPath = job.getProperty("assetPath", "");
            Resource resource = resolver.getResource(assetPath);
            if(resource!=null) {
                Asset asset = resource.adaptTo(Asset.class);
                if(asset!=null) {
                    Date localModDate = new Date(asset.getLastModified());

                    SimpleDateFormat sdf = new SimpleDateFormat(Constants.ISO_8601_24H_FULL_FORMAT);

                    final String videoJsonStr = job.getProperty("videoJson", "{}");
                    final JSONObject videoJson = new JSONObject(videoJsonStr);

                    Date remoteDate = sdf.parse(videoJson.getString(Constants.UPDATED_AT));

                    //LOCAL COMPARISON DATE TO SEE IF IT NEEDS TO UPDATE
                    if (localModDate.compareTo(remoteDate) < 0) {
                        if(log.isTraceEnabled()) {
                            log.trace("OLDERS-DATE>>>>>{}", localModDate);
                            log.trace("PARSED-DATE>>>>>{}", remoteDate);
                            log.trace("{} < {}", localModDate, remoteDate);
                            log.trace("MODIFICATION DETECTED");
                        }

                        final String accountId = job.getProperty("accountId", "");
                        ServiceUtil serviceUtil = new ServiceUtil(accountId);
                        serviceUtil.updateAsset(asset, videoJson, resolver, accountId);

                        log.info("finished updating video {}", asset.getPath());
                    } else {
                        log.trace("No Changes to be Made = Asset is equivalent");
                    }

                    return JobResult.OK;
                }
            }
        } catch (LoginException e) {
            log.error("failed to open service user resolver", e);
        }  catch (JSONException | ParseException e) {
            log.error("json error - failed while updating video.", e);
        } catch (RepositoryException | PersistenceException e) {
            log.error("repository error - failed while updating video.", e);
        }

        return JobResult.FAILED;
    }
}
