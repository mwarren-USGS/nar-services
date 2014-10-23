package gov.usgs.cida.nar.webservice;

import gov.usgs.cida.nar.service.SiteInformationService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("download")
public class DownloadService {
	private final static Logger LOG = LoggerFactory.getLogger(DownloadService.class);

	@GET
	@Path("/siteInfo/zip")
	@Produces("application/zip")
	public Response downloadZippedSiteInformation() throws NamingException {
		LOG.debug("Stream siteInfo/zipped started");
		
		return Response.ok(new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ZipOutputStream zip = new ZipOutputStream(output, StandardCharsets.UTF_8);
				zip.putNextEntry(new ZipEntry("siteInfo.csv"));
				SiteInformationService.streamData(zip);
				zip.closeEntry();
				zip.close();
			}
		}).header("Content-Disposition", "attachment; filename=\"siteInfo.zip\"").build();
	}
	
	@GET
	@Path("/siteInfo")
	@Produces(MediaType.TEXT_PLAIN)
	public StreamingOutput downloadSiteInformation() throws NamingException {
		LOG.debug("Streaming siteInfo/plain started");
		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				SiteInformationService.streamData(output);
			}
		};
	}

}
