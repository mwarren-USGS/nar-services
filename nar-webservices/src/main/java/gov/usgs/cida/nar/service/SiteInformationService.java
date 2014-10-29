package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.transform.NarOwsSiteToDelimitatedText;
import gov.usgs.cida.nar.util.JNDISingleton;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

public class SiteInformationService {
	public static final String SITE_ATTRIBUTE_TITLE = DownloadType.siteAttribute.getTitle();
	public static final String SITE_ATTRIBUTE_OUT_FILENAME = SITE_ATTRIBUTE_TITLE.replaceAll(" ", "_");
	
	private static final String SITE_INFO_URL_JNDI_NAME = "nar.endpoint.ows";
	private static final String SITE_LAYER_NAME = "NAR:JD_NFSN_sites0914";
	
	public void streamData(OutputStream output, 
			final List<String> format,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state) throws IOException {
		Client client = ClientBuilder.newClient();
		
		InputStream returnStream = (InputStream)client.target(buildSiteInfoRequest(siteType,
				stationId,
				state))
                .path("")
                .request(new MediaType[] {MediaType.APPLICATION_OCTET_STREAM_TYPE})
                .get(InputStream.class);
		
		new NarOwsSiteToDelimitatedText(",").transform(new InputStream[] { returnStream }, output);
	}
	
	private static String buildSiteInfoRequest( 
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state) {
		//TODO build OGC filter
		String filter = "";
		return JNDISingleton.getInstance().getProperty(SITE_INFO_URL_JNDI_NAME) + 
				"?service=WFS&version=1.0.0&request=GetFeature&typeName=" + SITE_LAYER_NAME + "&outputFormat=csv" + filter;
	}
}
