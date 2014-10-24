package gov.usgs.cida.nar.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DescriptionLoaderSingletonTest {

	@Test
	public void testGetDescription(){
		assertNonEmptyComments("test Description1");
		assertNonEmptyComments("Test description2");
		assertBadComments("bad DescriptionFile");
	}
	
	private void assertNonEmptyComments(String name) {
		String loadedDescription = DescriptionLoaderSingleton.getDescription(name);
		assertTrue("Text string found", loadedDescription.length() > 0);
		String[] lines = loadedDescription.split("\n");
		for(String line : lines) {
			assertTrue("Line starts with # char", line.startsWith("#"));
		}
	}
	
	private void assertBadComments(String name) {
		String loadedDescription = DescriptionLoaderSingleton.getDescription(name);
		assertTrue("Text string found", loadedDescription.length() > 0);
		String[] lines = loadedDescription.split("\n");
		boolean badLineFound = false;
		for(String line : lines) {
			if(!line.startsWith("#")) {
				badLineFound = true;
			}
		}
		assertTrue("Invalid comment found", badLineFound);
	}
}
