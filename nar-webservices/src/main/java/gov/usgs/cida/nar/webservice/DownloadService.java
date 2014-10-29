package gov.usgs.cida.nar.webservice;

import gov.usgs.cida.nar.service.DiscreteQwService;
import gov.usgs.cida.nar.service.DownloadServiceParameters;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nar.service.SiteInformationService;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.ServiceParameterUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("download")
public class DownloadService {
	private final static Logger LOG = LoggerFactory.getLogger(DownloadService.class);
	
	@GET
	@Path("/bundle/zip")
	@Produces("application/zip")
	public Response downloadZippedBundle(@Context final HttpServletRequest request) throws NamingException {
		LOG.debug("Stream full zipped bundle started");
		@SuppressWarnings("unchecked")
		final Map<String, String[]> params = request.getParameterMap();
		
		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ZipOutputStream zip = null;
				try {
					zip = new ZipOutputStream(output, StandardCharsets.UTF_8);
					addRequestSummaryEntry(zip, buildRequestDescriptionFromParams(params));
					
					if(ServiceParameterUtils.isSiteInformationRequested(params)) {
						addSiteInformationEntry(zip, params);
					}
					
					if(ServiceParameterUtils.isDiscreteQwRequested(params)) {
						addDiscreteQwEntry(zip, params);
					}

					if(ServiceParameterUtils.isAnnualLoadsRequested(params)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isMonthlyLoadsRequested(params)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isDailyFlowRequested(params)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isAnnualFlowRequested(params)) {
						//TODO hook up
					}

					if(ServiceParameterUtils.isMonthlyFlowRequested(params)) {
						//TODO hook up
					}
				} finally {
						IOUtils.closeQuietly(zip);
				}
			}
		}).header("Content-Disposition", "attachment; filename=\"data.zip\"").build();
	}

	@GET
	@Path("/siteAttributes")
	@Produces(MediaType.TEXT_PLAIN)
	public StreamingOutput downloadSiteInformation(@Context final HttpServletRequest request) throws NamingException {
		LOG.debug("Streaming siteInfo/plain started");
		@SuppressWarnings("unchecked")
		final Map<String, String[]> params = request.getParameterMap();
		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				new SiteInformationService().streamData(output, params);
			}
		};
	}
	
	private void addSiteInformationEntry(ZipOutputStream zip, Map<String, String[]> params) throws IOException {
		zip.putNextEntry(new ZipEntry(SiteInformationService.SITE_ATTRIBUTE_OUT_FILENAME + ".csv"));
		new SiteInformationService().streamData(zip, params);
		zip.closeEntry();
	}
	
	private void addDiscreteQwEntry(ZipOutputStream zip, Map<String, String[]> params) throws IOException {
		zip.putNextEntry(new ZipEntry(DiscreteQwService.DISCRETE_QW_OUT_FILENAME + ".csv"));
		new DiscreteQwService().streamData(zip, params);
		zip.closeEntry();
	}
	
	private void addRequestSummaryEntry(ZipOutputStream zip, String requestDescription) throws IOException {
		zip.putNextEntry(new ZipEntry("request.txt"));
		zip.write(requestDescription.getBytes());
		zip.closeEntry();
	}
	
	private String buildRequestDescriptionFromParams(Map<String, String[]> params) {
		StringBuffer sb = new StringBuffer();
		
		//List service criteria
		sb.append("Request criteria provided\n");
		boolean parameterPassed = false;
		for(DownloadServiceParameters parm : DownloadServiceParameters.values()) {
			if(params.keySet().contains(parm.name())) {
				sb.append(" - ");
				sb.append(parm.getTitle());
				sb.append(": ");
				sb.append(serializeParamaterList(params.get(parm.name())));
				sb.append("\n");
				parameterPassed = true;
			}
		}
		if(!parameterPassed) {
			sb.append("- NONE\n");
		}
		sb.append("\n\n");
		
		//List data headers which match request
		if(ServiceParameterUtils.isSiteInformationRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.siteAttribute);
		}

		if(ServiceParameterUtils.isDiscreteQwRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.discreteQw);
		}

		if(ServiceParameterUtils.isAnnualLoadsRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.annualLoad);
		}

		if(ServiceParameterUtils.isMonthlyLoadsRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.monthlyLoad);
		}

		if(ServiceParameterUtils.isDailyFlowRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.dailyFlow);
		}

		if(ServiceParameterUtils.isAnnualFlowRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.annualFlow);
		}

		if(ServiceParameterUtils.isMonthlyFlowRequested(params)) {
			appendDataTypeDescription(sb, DownloadType.monthlyFlow);
		}
		
		return sb.toString();
	}
	
	private void appendDataTypeDescription(StringBuffer sb, DownloadType t) {
		sb.append(t.getTitle());
		sb.append("\n");
		sb.append(DescriptionLoaderSingleton.getDescription(t.getTitle()));
		sb.append("\n\n");
	}
	
	private String serializeParamaterList(String[] params) {
		StringBuffer sb = new StringBuffer();
		
		for(String ps : params) {
			for(String p : ps.split(",")) {
				sb.append(p);
				sb.append(", ");
			}
		}
		//remove trailing ", "
		return sb.substring(0, sb.length() - 2);
	}
}
