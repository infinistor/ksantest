package com.pspace.jenkins;

import java.sql.*;

public class DBManager {
	final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBManager.class);

	private final int buildNumber;
	private final TestSuite testSuite;
	private final String tableName;
	private final DbConfig dbConfig;
	private final PlatformInfo platformInfo;

	public DBManager(DbConfig dbConfig, int buildNumber, TestSuite testSuite, String tableName,
			PlatformInfo platformInfo) {
		this.buildNumber = buildNumber;
		this.testSuite = testSuite;
		this.dbConfig = dbConfig;
		this.tableName = tableName;
		this.platformInfo = platformInfo;
	}

	private boolean createDetailTable(Connection connection) throws SQLException {
		var createTableQuery = "CREATE TABLE IF NOT EXISTS `" + tableName + "` ("
				+ "id int(11) NOT NULL AUTO_INCREMENT,"
				+ "build_id int(11) NOT NULL,"
				+ "class_name varchar(256) NOT NULL,"
				+ "case_name varchar(256) NOT NULL,"
				+ "result varchar(20) NOT NULL,"
				+ "error_type text DEFAULT NULL,"
				+ "message longtext DEFAULT NULL,"
				+ "content longtext DEFAULT NULL,"
				+ "system_out longtext DEFAULT NULL,"
				+ "system_err longtext DEFAULT NULL,"
				+ "times float NOT NULL,"
				+ "PRIMARY KEY (id)"
				+ ")";
		
		try (var stmt = connection.createStatement()) {
			stmt.execute(createTableQuery);
			return true;
		}
	}

	public boolean insert() {
		try (var connection = DriverManager.getConnection(
				dbConfig.getConnectionUrl(),
				dbConfig.getUser(),
				dbConfig.getPassword())) {

			// Create detail table if not exists
			if (!createDetailTable(connection)) {
				log.error("Failed to create detail table");
				return false;
			}

			// Insert test summary
			var summaryQuery = "INSERT INTO s3_summary (build_id, platform, os, gateway, env_type, language, sdk_version, tests, passed, failure, error, skipped, times, file_name) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			try (var stmt = connection.prepareStatement(summaryQuery)) {
				stmt.setInt(1, buildNumber);
				stmt.setString(2, platformInfo.getPlatform());
				stmt.setString(3, platformInfo.getOs());
				stmt.setString(4, platformInfo.getGateway());
				stmt.setString(5, platformInfo.getEnvType());
				stmt.setString(6, testSuite.getLanguage());
				stmt.setString(7, testSuite.getSdkVersion());
				stmt.setInt(8, testSuite.getTests());
				stmt.setInt(9, testSuite.getPassed());
				stmt.setInt(10, testSuite.getFailures());
				stmt.setInt(11, testSuite.getErrors());
				stmt.setInt(12, testSuite.getSkipped());
				stmt.setFloat(13, testSuite.getTimeAsFloat());
				stmt.setString(14, testSuite.getResultFileName());
				stmt.executeUpdate();
			}

			// Insert test details
			var detailQuery = "INSERT INTO `" + tableName
					+ "` (build_id, class_name, case_name, result, error_type, message, content, system_out, system_err, times)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			try (var stmt = connection.prepareStatement(detailQuery)) {
				// Set loop-invariant values
				stmt.setInt(1, buildNumber);

				for (var testCase : testSuite.getTestCases()) {
					stmt.setString(2, testCase.getClassname());
					stmt.setString(3, testCase.getName());
					stmt.setString(4, testCase.getResult());
					stmt.setString(5, testCase.getErrorType());
					stmt.setString(6, testCase.getErrorMessage());
					stmt.setString(7, testCase.getErrorContent());
					stmt.setString(8, testCase.getSystemOut());
					stmt.setString(9, testCase.getSystemErr());
					stmt.setFloat(10, testCase.getTimeAsFloat());
					stmt.addBatch();
				}
				stmt.executeBatch();
			}

			return true;
		} catch (SQLException e) {
			log.error("Database error", e);
			return false;
		}
	}
}