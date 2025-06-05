package com.pspace.jenkins;

import jakarta.xml.bind.annotation.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@XmlAccessorType(XmlAccessType.FIELD)
public class TestCase {
	@XmlAttribute
	private String name;

	@XmlAttribute
	private String classname;

	@XmlAttribute
	private double time;

	@XmlElement(name = "failure")
	private TestResult failure;

	@XmlElement(name = "error")
	private TestResult error;

	@XmlElement(name = "system-out")
	private String systemOut;

	@XmlElement(name = "system-err")
	private String systemErr;

	// DB 저장을 위한 메서드들
	public String getResult() {
		if (error != null) return "error";
		if (failure != null) return "failure";
		return "pass";
	}

	public String getErrorType() {
		TestResult result = error != null ? error : failure;
		return result != null ? result.getType() : null;
	}

	public String getErrorMessage() {
		TestResult result = error != null ? error : failure;
		return result != null ? result.getMessage() : null;
	}

	public String getErrorContent() {
		TestResult result = error != null ? error : failure;
		return result != null ? result.getContent() : null;
	}

	public float getTimeAsFloat() {
		return (float) time;
	}
}