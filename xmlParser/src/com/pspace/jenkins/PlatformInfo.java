package com.pspace.jenkins;

import lombok.Getter;

@Getter
public class PlatformInfo {
	private final String platform;
	private final String os;
	private final String gateway;
	private final String envType;
	private final String triggerType;

	public PlatformInfo(String platform, String os, String gateway, String envType, String triggerType) {
		this.platform = platform;
		this.os = os;
		this.gateway = gateway;
		this.envType = envType;
		this.triggerType = triggerType;
	}
}