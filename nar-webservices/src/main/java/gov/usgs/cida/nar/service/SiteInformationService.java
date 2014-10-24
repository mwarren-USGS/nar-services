package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;
import gov.usgs.cida.nar.util.JNDISingleton;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

public class SiteInformationService {
	public static final String SITE_ATTRIBUTE_TITLE = DownloadType.siteAttribute.getTitle();
	public static final String SITE_ATTRIBUTE_OUT_FILENAME = SITE_ATTRIBUTE_TITLE.replaceAll(" ", "_");
	
	private static final String SITE_INFO_URL_JNDI_NAME = "nar.endpoint.ows";
	private static final String SITE_LAYER_NAME = "NAR:JD_NFSN_sites0914";
	
	public static void streamData(OutputStream output, final Map<String, String[]> params) throws IOException {
		Client client = ClientBuilder.newClient();
		InputStream returnStream = (InputStream)client.target(buildSiteInfoRequest())
                .path("")
                .request(new MediaType[] {MediaType.APPLICATION_OCTET_STREAM_TYPE})
                .get(InputStream.class);
		
		//write description comments
		output.write(DescriptionLoaderSingleton.getDescription(SITE_ATTRIBUTE_TITLE).getBytes());
		output.write("\n".getBytes());
		
		//TODO will have to read this line by line to transform the CSV
		int nextByte = returnStream.read();
		do {
			output.write(nextByte);
			nextByte = returnStream.read();
		} while (nextByte >= 0);
	}
	
	private static String buildSiteInfoRequest() {
		return JNDISingleton.getInstance().getProperty(SITE_INFO_URL_JNDI_NAME) + 
				"?service=WFS&version=1.0.0&request=GetFeature&typeName=" + SITE_LAYER_NAME + "&outputFormat=csv";
	}
}
