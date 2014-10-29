package gov.usgs.cida.nar.transform;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class NarSosTimeSeriesToDelimitedText implements INarTransform {

	private String delimiter = ",";
	
	public NarSosTimeSeriesToDelimitedText(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public void transform(InputStream[] input, OutputStream output) throws IOException {
		for (InputStream in : input) {
			IOUtils.copy(in, output);
		}
	}

}
