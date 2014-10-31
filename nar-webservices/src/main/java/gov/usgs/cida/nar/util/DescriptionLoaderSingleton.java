package gov.usgs.cida.nar.util;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads text files containing descriptions of data files
 * @author thongsav
 *
 */
public class DescriptionLoaderSingleton {
	private final static Logger LOG = LoggerFactory.getLogger(DescriptionLoaderSingleton.class);
	private static HashMap<String, String> descriptionCache = new HashMap<>();
	
	public static String getDescription(String name) {
		if(descriptionCache.containsKey(name)) {
			return descriptionCache.get(name);
		} else {
			return loadDescription(name);
		}
	}
	
	private static String loadDescription(String name) {
		String filename = name.replaceAll(" ",  "_") + ".txt";
		try {
			String description = IOUtils.toString(DescriptionLoaderSingleton.class.getResourceAsStream("descriptions/" + filename));
			descriptionCache.put(name, description + "\r\n");
		} catch (IOException e) {
			LOG.info("Error attempting to read file: " + filename);
		}
	    
		return descriptionCache.get(name);
	}
}
