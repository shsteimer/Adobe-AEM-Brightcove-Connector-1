package com.coresecure.brightcove.wrapper.sync;

import com.coresecure.brightcove.wrapper.objects.BinaryObj;
import com.coresecure.brightcove.wrapper.sling.ConfigurationGrabber;
import com.coresecure.brightcove.wrapper.sling.ConfigurationService;
import com.coresecure.brightcove.wrapper.sling.ServiceUtil;
import com.coresecure.brightcove.wrapper.utils.Constants;
import com.coresecure.brightcove.wrapper.utils.HttpServices;
import com.coresecure.brightcove.wrapper.utils.ImageUtil;
import com.day.cq.commons.jcr.JcrUtil;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.AssetManager;
import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.commons.mime.MimeTypeService;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.jcodec.api.awt.AWTSequenceEncoder;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

@Component(service = JobConsumer.class, immediate = true, property = {
        JobConsumer.PROPERTY_TOPICS + "=" + BrightcoveSynchronizationNewVideoImportJob.JOB_TOPIC
})
public class BrightcoveSynchronizationNewVideoImportJob implements JobConsumer {

    public static final String JOB_TOPIC = BrightcoveSynchronizationScheduledJob.JOB_TOPIC + "/new";

    private static final Logger log = LoggerFactory.getLogger(BrightcoveSynchronizationNewVideoImportJob.class);

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private ConfigurationGrabber configurationGrabber;

    @Reference
    MimeTypeService mType;

    @Override
    @SuppressWarnings("squid:CallToDeprecatedMethod")
    public JobResult process(Job job) {
        Map<String, Object> bcServiceParams = Collections.singletonMap(ResourceResolverFactory.SUBSERVICE,
                Constants.SERVICE_ACCOUNT_IDENTIFIER);
        try (ResourceResolver resolver = resourceResolverFactory.getServiceResourceResolver(bcServiceParams)) {
            String videoJson = job.getProperty("videoJson", "{}");
            String accountId = job.getProperty("accountId", "");
            String brightcoveVideoId = job.getProperty("brightcoveVideoId", "");

            ConfigurationService configurationService = configurationGrabber.getConfigurationService(accountId);
            if(configurationService!=null) {
                String accountFolderPath = createAccountFolder(resolver, accountId, configurationService);
                if(accountFolderPath == null) {
                    log.error("failed to create account folder");
                    return JobResult.FAILED;
                }
                String bcFileName = brightcoveVideoId + ".mp4";

                JSONObject videoJsonObject = new JSONObject(videoJson);
                Asset asset = createAssetForVideo(resolver, accountFolderPath, bcFileName, brightcoveVideoId, videoJsonObject, configurationService);
                if(asset!=null) {
                    ServiceUtil serviceUtil = new ServiceUtil(accountId);
                    serviceUtil.updateAsset(asset, videoJsonObject, resolver, accountId);
                }
            }

            resolver.commit();
        } catch (LoginException e) {
            log.error("failed to open service user resolver", e);
        } catch (RepositoryException | PersistenceException e) {
            log.error("repository error - failed while creating video.", e);
        } catch (JSONException e) {
            log.error("json error - failed while creating video.", e);
        } catch (IOException e) {
            log.error("I/O error - failed while creating video.", e);
        }

        return JobResult.OK;
    }

    private Asset createAssetForVideo(ResourceResolver resolver, String accountFolderPath, String bcFileName, String brightcoveVideoId, JSONObject videoJsonObject, ConfigurationService configurationService) throws IOException, JSONException, RepositoryException {
        AssetManager assetManager = resolver.adaptTo(AssetManager.class);
        if(assetManager!=null) {
            String mimeType = null;
            InputStream binaryIns = null;
            InputStream ins = null;
            FileInputStream fis = null;
            try {
                String thumbnailSrc = getItemFromJson(videoJsonObject,Constants.THUMBNAIL_URL);
                BinaryObj binaryRes = null;
                if (HttpServices.isLocalPath(thumbnailSrc)) {
                    //HAS LOCAL THUMB PATH

                    //IF THE THUMBNAIL SOURCE IST /CONTENT/DAM/ IT IS LOCAL - IF LOCAL >>
                    log.trace("->>Pulling local image as this video's thumbnail image binary");
                    log.trace("->>Thumbnail Source is/: {}", thumbnailSrc);

                    binaryRes = com.coresecure.brightcove.wrapper.utils.JcrUtil.getLocalBinary(resolver, thumbnailSrc, mType);
                } else {
                    log.trace("->>Pulling external image as this video's thumbnail image binary - Must do a GET");
                    log.trace("->>Thumbnail Source is/: {}", thumbnailSrc);
                    binaryRes = HttpServices.getRemoteBinary(thumbnailSrc, "", null);
                    log.trace("->>[PULLING THUMBNAIL] : {}", thumbnailSrc);
                }
                if (binaryRes.binary != null) {
                    binaryIns = binaryRes.binary;
                    mimeType = binaryRes.mime_type;
                } else {
                    binaryRes = com.coresecure.brightcove.wrapper.utils.JcrUtil.getLocalBinary(resolver, "/etc/designs/cs/brightcove/shared/img/noThumbnail.jpg", mType);
                    if (binaryRes.binary != null) {
                       binaryIns = binaryRes.binary;
                        mimeType = binaryRes.mime_type;
                    } else {
                        log.trace("FAIL EXTERNAL");
                        return null;
                    }
                }

                BufferedImage image = ImageIO.read(binaryIns);

                //CRATE TEMPORARY MP4 FILE
                String prefix = brightcoveVideoId + "-";
                String suffix = ".mp4";
                File tempFile2 = File.createTempFile(prefix, suffix);
                tempFile2.deleteOnExit();

                AWTSequenceEncoder enc = AWTSequenceEncoder.createSequenceEncoder(tempFile2, 30);

                log.trace("ENCODING PROCESS");
                log.trace("height: {}" ,image.getType());
                log.trace("height: {}" ,image.getHeight());
                log.trace("width: {}" ,image.getWidth());

                if (image.getHeight() % 2 != 0) {
                    image = ImageUtil.cropImage(image, new Rectangle(image.getWidth(), image.getHeight() - (image.getHeight() % 2)));
                }
                if (image.getWidth() % 2 != 0) {
                    image = ImageUtil.cropImage(image, new Rectangle(image.getWidth() - (image.getWidth() % 2), image.getHeight()));
                }

                enc.encodeImage(image);
                enc.finish();

                fis = new FileInputStream(tempFile2);
                ins = new BufferedInputStream(fis);

                Asset asset = assetManager.createAsset(accountFolderPath + "/" + bcFileName, ins, mimeType, false);

                resolver.commit();
                return asset;
            } finally {
                IOUtils.closeQuietly(binaryIns);
                IOUtils.closeQuietly(ins);
                IOUtils.closeQuietly(fis);
            }


        }

        return null;
    }

    private String getItemFromJson(JSONObject innerObj, String key) throws JSONException{
        return innerObj.has(key) && innerObj.get(key)!=null ? innerObj.getString(key) : "";
    }

    private String createAccountFolder(ResourceResolver resourceResolver, String accountId, ConfigurationService configurationService) throws RepositoryException {
        Session session = resourceResolver.adaptTo(Session.class);
        if(session!=null) {
            final String confPath = configurationService.getAssetIntegrationPath();                     //GET PRECONFIGURED SYNC DAM TARGET PATH
            final String basePath = (confPath.endsWith("/") ? confPath : confPath.concat("/")).concat(accountId).concat("/"); //CREATE BASE PATH
            //CREATE AND NAME BRIGHTCOVE ASSET FOLDERS PER ACCOUNT
            Node accountFolder = JcrUtil.createPath(basePath, "sling:OrderedFolder", session);
            accountFolder.setProperty("jcr:title", configurationService.getAccountAlias());
            session.save();

            return accountFolder.getPath();
        }

        return null;
    }
}
