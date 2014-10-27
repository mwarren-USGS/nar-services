package gov.usgs.cida.nar.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * This interface orchestrates the data streaming and transformation of multiple intput data streams
 * to a single output stream.
 * 
 * @author thongsav
 *
 */
public interface INarStreamService {
	/**
	 * Creates/configures several input streams.
	 * 
	 * TODO: this will likely need to change to interface with NUDE framework, by wrapping input
	 * streams into parsers which produce ResultSet objects. The ResultSet objects need to be the 
	 * feed into the NUDE framework.
	 * 
	 * @param output the output stream to funnel the final results to
	 * @param params HTTP params, of key-value array pairs. Individual values may be comma separated.
	 * @throws IOException
	 */
	public void streamData(OutputStream output, final Map<String, String[]> params) throws IOException;
}
