package org.example.s3tests;

public class DBConfig {
	public String host;
	public String port;
	public String user;
	public String password;
	public String database;

	public DBConfig(String host, String port, String user, String password, String database) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.database = database;
	}
}
