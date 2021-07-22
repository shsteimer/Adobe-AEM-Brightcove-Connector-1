/*

    Adobe AEM Brightcove Connector

    Copyright (C) 2018 Coresecure Inc.

    Authors:    Alessandro Bonfatti
                Yan Kisen
                Pablo Kropilnicki

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    - Additional permission under GNU GPL version 3 section 7
    If you modify this Program, or any covered work, by linking or combining
    it with httpclient 4.1.3, httpcore 4.1.4, httpmine 4.1.3, jsoup 1.7.2,
    squeakysand-commons and squeakysand-osgi (or a modified version of those
    libraries), containing parts covered by the terms of APACHE LICENSE 2.0
    or MIT License, the licensors of this Program grant you additional
    permission to convey the resulting work.

 */
package com.coresecure.brightcove.wrapper.webservices;


//*Imports*//

import com.coresecure.brightcove.wrapper.sync.BrightcoveSynchronizationScheduledJob;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.commons.mime.MimeTypeService;
import org.apache.sling.event.jobs.JobManager;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.propertytypes.ServiceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


@Component(service = { Servlet.class },
    property = {
        "sling.servlet.extensions=html",
        "sling.servlet.paths=/bin/brightcove/dataload"
    }
)
@ServiceDescription("Brightcove Data Load Servlet")
public class AssetPropertyIntegrator extends SlingAllMethodsServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssetPropertyIntegrator.class);

    @Reference
    private transient MimeTypeService mType;

    @Reference
    private transient ResourceResolverFactory resourceResolverFactory;

    @Reference
    private JobManager jobManager;

    @Override
    protected void doPost(final SlingHttpServletRequest req, final SlingHttpServletResponse resp) throws ServletException, IOException {
        executeRequest(req, resp);
    }

    private void executeRequest(final SlingHttpServletRequest req, final SlingHttpServletResponse resp) {
        LOGGER.info("firing the asset import job, actual import will be done there async.");
        Map<String, Object> props = new HashMap<>();
        jobManager.addJob(BrightcoveSynchronizationScheduledJob.JOB_TOPIC, props);
    }

    @Override
    protected void doGet(final SlingHttpServletRequest req, final SlingHttpServletResponse resp) throws ServletException, IOException {
        executeRequest(req, resp);
    }

}
