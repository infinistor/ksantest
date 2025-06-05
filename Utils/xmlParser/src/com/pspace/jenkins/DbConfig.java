package com.pspace.jenkins;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DbConfig {
	private String host;
	private int port;
	private String database;
	private String user;
	private String password;

	public String getConnectionUrl() {
		return String.format("jdbc:mariadb://%s:%d/%s", host, port, database);
	}
}