package com.pspace.jenkins;

import jakarta.xml.bind.annotation.*;
import lombok.Getter;
import lombok.Setter;
import java.io.File;
import java.util.List;

@Getter
@Setter
@XmlRootElement(name = "testsuite")
@XmlAccessorType(XmlAccessType.FIELD)
public class TestSuite {
	@XmlAttribute
	private int tests;

	@XmlAttribute
	private int errors;

	@XmlAttribute
	private int skipped;

	@XmlAttribute
	private int failures;

	@XmlAttribute
	private double time;

	@XmlElement(name = "testcase")
	private List<TestCase> testCases;

	@XmlElement(name = "properties")
	private Properties properties;

	private String resultFileName;

	// Additional business logic methods
	public int getPassed() {
		return tests - failures - errors - skipped;
	}

	public float getTimeAsFloat() {
		return (float) time;
	}

	public void setSourceFile(String filePath) {
		this.resultFileName = parseFileName(filePath);
	}

	private String parseFileName(String filePath) {
		int index = filePath.lastIndexOf(File.separator) + 1;
		return filePath.substring(index).replace(".xml", "");
	}

	public String getLanguage() {
		if (properties != null && properties.getProperties() != null) {
			for (Property prop : properties.getProperties()) {
				if ("java.specification.version".equals(prop.getName())) {
					return "Java";
				}
				// 다른 언어 타입도 추가 가능
			}
		}
		return "Unknown";
	}

	public String getSdkVersion() {
		if (properties != null && properties.getProperties() != null) {
			for (Property prop : properties.getProperties()) {
				if ("java.class.path".equals(prop.getName())) {
					String classPath = prop.getValue();
					// AWS SDK v2 버전 찾기
					int sdkIndex = classPath.indexOf("/software/amazon/awssdk/s3/");
					if (sdkIndex != -1) {
						int startIndex = sdkIndex + "/software/amazon/awssdk/s3/".length();
						int endIndex = classPath.indexOf("/", startIndex);
						if (endIndex != -1) {
							return classPath.substring(startIndex, endIndex);
						}
					}
				}
			}
		}
		return "Unknown";
	}
}