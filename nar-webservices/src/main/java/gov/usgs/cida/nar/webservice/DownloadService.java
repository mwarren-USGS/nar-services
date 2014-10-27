package gov.usgs.cida.nar.webservice;

import gov.usgs.cida.nar.service.DownloadServiceParameters;
import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nar.service.SiteInformationService;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;

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
					addRequestSummaryEntry(zip, buildRequestDescriptionFromParams(params)); //TODO build useful information about the request
					addSiteInformationEntry(zip, params);
				} finally {
					try {
						zip.close();
					} catch (Exception e) {
						LOG.warn("Unhandled exception closing zip file", e);
					}
				}
			}
		}).header("Content-Disposition", "attachment; filename=\"data.zip\"").build();
	}
	
	@GET
	@Path("/siteAttributes/zip")
	@Produces("application/zip")
	public Response downloadZippedSiteInformation(@Context final HttpServletRequest request) throws NamingException {
		LOG.debug("Stream siteInfo/zipped started");
		@SuppressWarnings("unchecked")
		final Map<String, String[]> params = request.getParameterMap();
		
		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ZipOutputStream zip = null;
				try {
					zip = new ZipOutputStream(output, StandardCharsets.UTF_8);
					addRequestSummaryEntry(zip, buildRequestDescriptionFromParams(params)); //TODO build useful information about the request
					addSiteInformationEntry(zip, params);
				} finally {
					try {
						zip.close();
					} catch (Exception e) {
						LOG.warn("Unhandled exception closing zip file", e);
					}
				}
			}
		}).header("Content-Disposition", "attachment; filename=\"" + SiteInformationService.SITE_ATTRIBUTE_OUT_FILENAME +".zip\"").build();
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
		
		//List data headers (TODO listing all for now)
		for(DownloadType t : DownloadType.values()) {
			sb.append(t.getTitle());
			sb.append("\n");
			sb.append(DescriptionLoaderSingleton.getDescription(t.getTitle()));
			sb.append("\n\n");
		}
		
		return sb.toString();
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
