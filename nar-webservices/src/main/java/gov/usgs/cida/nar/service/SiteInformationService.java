package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.util.JNDISingleton;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

public class SiteInformationService {
	private static final String SITE_INFO_URL_JNDI_NAME = "nwc.endpoint.ows";
	
	public static void streamData(OutputStream output) throws IOException {
		Client client = ClientBuilder.newClient();
		InputStream returnStream = (InputStream)client.target(buildSiteInfoRequest())
                .path("")
                .request(new MediaType[] {MediaType.APPLICATION_OCTET_STREAM_TYPE})
                .get(InputStream.class);
		
		int nextByte = returnStream.read();
		do {
			output.write(nextByte);
			nextByte = returnStream.read();
		} while (nextByte >= 0);
	}
	
	private static String buildSiteInfoRequest() {
		return JNDISingleton.getInstance().getJNDIPropertyUsingContexts(SITE_INFO_URL_JNDI_NAME) + 
				"?service=WFS&version=1.0.0&request=GetFeature&typeName=NAR:JD_NFSN_sites0914&outputFormat=csv";
	}
}
