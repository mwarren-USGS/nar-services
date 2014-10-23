package gov.usgs.cida.nar.service;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

public class SiteInformationService {
	public static void streamData(OutputStream output) throws IOException {
		MessageFormat form = new MessageFormat(
			     "\"{0}\",\"{1}\"");
		
		output.write("header1,header2".getBytes());
		output.write("\n".getBytes());
		for(int i = 0; i < 100000; i++) {
			output.write(form.format(new String[] {String.valueOf(i), String.valueOf(i+1)}).getBytes());
			output.write("\n".getBytes());
		}
	}
}
