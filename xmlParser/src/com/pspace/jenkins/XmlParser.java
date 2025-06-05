package com.pspace.jenkins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.xml.bind.JAXBContext;
import java.io.File;

public class XmlParser {
	private static final Logger log = LoggerFactory.getLogger(XmlParser.class);
	private final String fileName;
	private TestSuite testSuite;

	public XmlParser(String fileName) {
		this.fileName = fileName;
	}

	public boolean read() {
		try {
			var context = JAXBContext.newInstance(TestSuite.class);
			var unmarshaller = context.createUnmarshaller();
			testSuite = (TestSuite) unmarshaller.unmarshal(new File(fileName));
			if (testSuite != null) {
				testSuite.setSourceFile(fileName);
			}
			return true;
		} catch (Exception e) {
			log.error("Error parsing XML file: " + fileName, e);
			return false;
		}
	}

	public TestSuite getTestSuite() {
		return testSuite;
	}
}