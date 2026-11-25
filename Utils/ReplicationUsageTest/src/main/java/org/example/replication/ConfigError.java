package org.example.replication;

/** 설정·포털 점검 오류. 종료 코드 2 로 끝난다. */
public class ConfigError extends RuntimeException {
	public ConfigError(String message) {
		super(message);
	}
}
