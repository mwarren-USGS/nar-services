package gov.usgs.cida.nar.service;

import gov.usgs.cida.nar.transform.NarSosTimeSeriesToDelimitedText;
import gov.usgs.cida.nar.util.JNDISingleton;
import gov.usgs.cida.nar.util.ServiceParameterUtils;
import gov.usgs.webservices.framework.basic.MimeType;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.lang.String.format;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class DiscreteQwService {
	
	public static final String DISCRETE_QW_TITLE = DownloadType.discreteQw.getTitle();
	public static final String DISCRETE_QW_OUT_FILENAME = DISCRETE_QW_TITLE.replaceAll(" ", "_");
	
	private static final String DISCRETE_QW_URL_JNDI_NAME = "nar.endpoint.sos";

	public void streamData(OutputStream output,
			final MimeType mimeType,
			final List<String> qwDataType,
			final List<String> constituent,
			final List<String> siteType,
			final List<String> stationId,
			final List<String> state,
			final List<String> startDateTime,
			final List<String> endDateTime) throws IOException {
		Client client = ClientBuilder.newClient();
		// TODO do this proper
		Response response = client.target(buildDiscreteQwEndpoint())
				.path("")
				.request(new MediaType[] {MediaType.APPLICATION_JSON_TYPE})
				.post(buildDiscreteQwPost(null));
		try (InputStream returnStream = response.readEntity(InputStream.class)) {
			//new NarSosTimeSeriesToDelimitedText(mimeType).transform(new InputStream[] { returnStream }, output);
		}
	}
	
	private static Entity buildDiscreteQwPost(final Map<String, String[]> params) {
		StringBuilder sb = new StringBuilder();
		
		// TODO replace this with contents of params
		sb.append("{")
			.append("\"request\": \"GetResult\",")
			.append("\"service\": \"SOS\",")
			.append("\"version\": \"2.0.0\",")
			.append("\"offering\": \"http://cida.usgs.gov/def/NAR/procedure/TP\",")
			.append("\"observedProperty\": \"http://cida.usgs.gov/def/NAR/property/TP/discrete\",")
			.append("\"featureOfInterest\": \"01170100\",")
			.append("\"temporalFilter\": [")
			.append("{")
			.append("\"during\": {")
			.append("\"ref\": \"om:phenomenonTime\",")
			.append("\"value\": [")
			.append("\"2012-01-01T00:00:00Z\",")
			.append("\"2012-12-31T23:59:59Z\"")
			.append("]")
			.append("}")
			.append("}")
			.append("]")
			.append("}");
		
		return Entity.json(sb.toString());
	}
	
	private static String buildDiscreteQwEndpoint() {
		return JNDISingleton.getInstance().getProperty(DISCRETE_QW_URL_JNDI_NAME) + "/json";
	}

}
