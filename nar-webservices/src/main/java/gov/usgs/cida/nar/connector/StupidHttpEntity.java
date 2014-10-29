package gov.usgs.cida.nar.connector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;

/**
 *
 * @author Jordan Walker <jiwalker@usgs.gov>
 */
public class StupidHttpEntity implements HttpEntity {
	
	private InputStream input;
	
	public StupidHttpEntity(InputStream input) {
		this.input = input;
	}

	@Override
	public boolean isRepeatable() {
		return false;
	}

	@Override
	public boolean isChunked() {
		return true;
	}

	@Override
	public long getContentLength() {
		return -1l;
	}

	@Override
	public Header getContentType() {
		return null;
	}

	@Override
	public Header getContentEncoding() {
		return null;
	}

	@Override
	public InputStream getContent() throws IOException, IllegalStateException {
		return input;
	}

	@Override
	public void writeTo(OutputStream out) throws IOException {
		throw new IOException("I'm stupid");
	}

	@Override
	public boolean isStreaming() {
		return true;
	}

	@Override
	public void consumeContent() throws IOException {
		IOUtils.closeQuietly(input);
	}
	
}
