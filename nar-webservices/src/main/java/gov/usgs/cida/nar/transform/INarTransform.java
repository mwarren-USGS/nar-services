package gov.usgs.cida.nar.transform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides a transform method for streaming data.
 * @author thongsav
 *
 */
public interface INarTransform {
	/**
	 * Reads and transforms data from array of input streams, writing results straight to output.
	 * 
	 * @param input input stream providing text input
	 * @param output where to write text output
	 * @throws IOException
	 */
	public void transform(InputStream input[], OutputStream output) throws IOException;
}
