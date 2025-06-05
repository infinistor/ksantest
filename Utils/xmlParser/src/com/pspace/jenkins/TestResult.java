package com.pspace.jenkins;

import jakarta.xml.bind.annotation.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@XmlAccessorType(XmlAccessType.FIELD)
public class TestResult {
	@XmlAttribute
	private String type;

	@XmlAttribute
	private String message;

	@XmlValue
	private String content;
}