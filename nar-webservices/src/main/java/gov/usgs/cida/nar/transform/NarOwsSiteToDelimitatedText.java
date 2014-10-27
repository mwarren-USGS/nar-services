package gov.usgs.cida.nar.transform;

import gov.usgs.cida.nar.service.DownloadType;
import gov.usgs.cida.nar.util.DescriptionLoaderSingleton;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NarOwsSiteToDelimitatedText implements INarTransform {
	public static final String SITE_ATTRIBUTE_TITLE = DownloadType.siteAttribute.getTitle();
	
	private String delimiter = ","; 
	public NarOwsSiteToDelimitatedText(String delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public void transform(InputStream[] input, OutputStream output)
			throws IOException {
		//write description comments
		output.write(DescriptionLoaderSingleton.getDescription(SITE_ATTRIBUTE_TITLE).getBytes());
		output.write("\n".getBytes());
		
		if(input.length > 1) {
			throw new RuntimeException("Site transform can only have a single input stream");
		}
		//TODO will have to read this line by line to transform the CSV
		int nextByte = input[0].read();
		do {
			output.write(nextByte);
			nextByte = input[0].read();
		} while (nextByte >= 0);
	}

}
