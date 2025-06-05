package com.pspace.jenkins;

import jakarta.xml.bind.annotation.*;
import lombok.Getter;
import lombok.Setter;
import java.util.List;

@Getter
@Setter
@XmlAccessorType(XmlAccessType.FIELD)
public class Properties {
	@XmlElement(name = "property")
	private List<Property> properties;
}

@Getter
@Setter
@XmlAccessorType(XmlAccessType.FIELD)
class Property {
	@XmlAttribute
	private String name;

	@XmlAttribute
	private String value;
}