package com.coresecure.brightcove.wrapper.sync;

import com.coresecure.brightcove.wrapper.config.BrightcoveAccountService;
import com.coresecure.brightcove.wrapper.sling.ConfigurationGrabber;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.coresecure.brightcove.wrapper.sling.ServiceUtil;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.day.cq.commons.jcr.JcrConstants;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.search.PredicateGroup;
import com.day.cq.search.Query;
import com.day.cq.search.QueryBuilder;
import com.day.cq.search.result.Hit;
import com.day.cq.search.result.SearchResult;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobBuilder;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.ScheduledJobInfo;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component(service = JobConsumer.class, immediate = true, property = {
        JobConsumer.PROPERTY_TOPICS + "=" + BrightcoveSynchronizationScheduledJob.JOB_TOPIC
}, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = BrightcoveSynchronizationScheduledJob.Configuration.class)
public class BrightcoveSynchronizationScheduledJob implements JobConsumer {

    public static final String JOB_TOPIC = "brightcove/assets/sync";

    private static final Logger log = LoggerFactory.getLogger(BrightcoveSynchronizationScheduledJob.class);

    @Reference
    private JobManager jobManager;

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private ConfigurationGrabber configurationGrabber;

    @Reference
    private QueryBuilder queryBuilder;

    @Reference
    private BrightcoveAccountService accountService;

    @Override
    @SuppressWarnings("squid:CallToDeprecatedMethod")
    public JobResult process(Job job) {

        final boolean shouldImportNewVideos = job.getProperty("shouldImportNewVideos", false);
        final int queryCursorSize = job.getProperty("queryCursorSize", 100);
        final int maxAllowedVideos = job.getProperty("maxAllowedVideos", 100000);

        Map<String, Object> bcServiceParams = Collections.singletonMap(ResourceResolverFactory.SUBSERVICE,
                                        Constants.SERVICE_ACCOUNT_IDENTIFIER);
        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(bcServiceParams)) {
            Set<String> availableServices = configurationGrabber.getAvailableServices();
            for (String accountId : availableServices) {
                ConfigurationService configurationService = configurationGrabber.getConfigurationService(accountId);
                if (configurationService != null) {
                    ServiceUtil serviceUtil = new ServiceUtil(configurationService.getAccountID());

                    int offset = 0;
                    boolean moreVideos = true;
                    while (moreVideos) {
                        JSONObject jsonObject = new JSONObject(serviceUtil.searchVideo("", offset, queryCursorSize, Constants.NAME, true));
                        final JSONArray itemsArr = jsonObject.getJSONArray("items");

                        offset += queryCursorSize;
                        moreVideos = itemsArr.length() > 0;

                        processVideoResults(shouldImportNewVideos, resolver, accountId, itemsArr);

                        if(offset >= maxAllowedVideos) {
                            //this amounts to little more than a sanity check to avoid an infinite loop
                            log.warn("exceeded max number of videos, quitting");
                            moreVideos = false;
                        }
                    }
                }
            }

            return JobResult.OK;
        } catch (LoginException e) {
            log.error("failed to open service user resolver", e);
        } catch (JSONException | ParseException | RepositoryException e) {
            log.error("failed while retrieving and updating videos.", e);
        }

        return JobResult.FAILED;
    }

    @SuppressWarnings("squid:CallToDeprecatedMethod")
    private void processVideoResults(boolean shouldImportNewVideos, ResourceResolver resolver, String accountId, JSONArray itemsArr) throws JSONException, RepositoryException, ParseException {
        for (int i = 0; i < itemsArr.length(); i++) {
            final JSONObject innerObj = itemsArr.getJSONObject(i);
            final String brightcoveVideoId = innerObj.optString(Constants.ID, "");

            Map<String, Object> props = new HashMap<>();

            props.put("videoJson", innerObj.toString());
            props.put("accountId", accountId);
            props.put("brightcoveVideoId", brightcoveVideoId);

            final Asset asset = findAssetForBcVideo(resolver, brightcoveVideoId, accountId);
            if (asset == null) {
                if (shouldImportNewVideos) {
                    //fire job to import new video
                    log.debug("firing job to import new video {} from account {}", brightcoveVideoId, accountId);
                    jobManager.addJob(JOB_TOPIC + "/new", props);
                } else {
                    log.debug("skipping import of video {} due to config", brightcoveVideoId);
                }
            } else {
                if(requiresUpdate(asset, innerObj)) {
                    //fire job to update video
                    props.put("assetPath", asset.getPath());

                    log.debug("firing job to update video {}-{} from account {}", asset.getPath(), brightcoveVideoId, accountId);
                    jobManager.addJob(JOB_TOPIC + "/update", props);
                }
            }
        }
    }

    @SuppressWarnings("squid:CallToDeprecatedMethod")
    private boolean requiresUpdate(Asset asset, JSONObject innerObj) throws JSONException, ParseException {
        Date localModDate = new Date(asset.getLastModified());

        SimpleDateFormat sdf = new SimpleDateFormat(Constants.ISO_8601_24H_FULL_FORMAT);
        Date remoteDate = sdf.parse(innerObj.getString(Constants.UPDATED_AT));

        //LOCAL COMPARISON DATE TO SEE IF IT NEEDS TO UPDATE
        if (localModDate.compareTo(remoteDate) < 0) {
            log.debug("remote video update date is after asset update date, so firing job to update.");
            return true;
        }

        return false;
    }

    private Asset findAssetForBcVideo(ResourceResolver resolver, String brightcoveVideoId, String accountId) throws RepositoryException {
        Map<String, String> predicates = new HashMap<>();

        predicates.put("type", DamConstants.NT_DAM_ASSET);
        predicates.put("path", DamConstants.MOUNTPOINT_ASSETS);

        predicates.put("1_property", JcrConstants.JCR_CONTENT + "/" + DamConstants.METADATA_FOLDER + "/" + Constants.BRC_ID);
        predicates.put("1_property.value", brightcoveVideoId);

        Query query = queryBuilder.createQuery(PredicateGroup.create(predicates), resolver.adaptTo(Session.class));
        SearchResult result = query.getResult();
        for (Hit hit : result.getHits()) {
            Resource resource = hit.getResource();
            ConfigurationService service = accountService.getService(resource);
            if(service!=null && service.getAccountID().equals(accountId)) {
                Asset asset = resource.adaptTo(Asset.class);
                if (asset != null) {
                    return asset;
                }
            } else {
                log.debug("found a video at {} with id {}, but not for the proper account id {}", resource.getPath(), brightcoveVideoId, accountId);
            }
        }

        return null;
    }

    @Activate
    protected void activate(Configuration config) {
        String schedulingExpression = config.scheduler_expression();
        JobBuilder jobBuilder = jobManager.createJob(JOB_TOPIC);

        Map<String, Object> jobProps = new HashMap<>();

        jobProps.put("shouldImportNewVideos", config.should_import_new_videos());
        jobProps.put("queryCursorSize", config.query_cursor_size());
        jobProps.put("maxAllowedVideos", config.max_videos_to_process());

        jobBuilder.properties(jobProps);

        JobBuilder.ScheduleBuilder scheduleBuilder = jobBuilder.schedule();

        scheduleBuilder.cron(schedulingExpression);

        ScheduledJobInfo scheduledJobInfo = scheduleBuilder.add();
        if (scheduledJobInfo == null) {
            log.error("failed to add job.");
        } else {
            log.debug("Brightcove Sync Job Scheduled");
        }
    }

    @Deactivate
    protected void deactivate(Configuration config) {
        Collection<ScheduledJobInfo> scheduledJobInfos = jobManager.getScheduledJobs(JOB_TOPIC, 0, null);
        for (ScheduledJobInfo scheduledJobInfo : scheduledJobInfos) {
            log.debug("unscheduling job by name {} from topic.", JOB_TOPIC);
            scheduledJobInfo.unschedule();
        }
    }

    @ObjectClassDefinition(name = "Brightcove Sync Scheduler")
    public @interface Configuration {

        @AttributeDefinition(description = "Cron Scheduling Expression")
        String scheduler_expression() default "";

        @AttributeDefinition(description = "Should new videos be imported to the DAM?")
        boolean should_import_new_videos() default false;

        int query_cursor_size() default 100;

        int max_videos_to_process() default 100000;
    }
}
