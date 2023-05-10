package org.example.Utility;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MariaDB {
	private static HikariConfig config = new HikariConfig();
	private static HikariDataSource ds;

	private MariaDB() {
		;
	}

	public static MariaDB getInstance() {
		return LazyHolder.INSTANCE;
	}

	private static class LazyHolder {
		private static final MariaDB INSTANCE = new MariaDB();
	}

	public List<HashMap<String, Object>> select(String query, List<Object> params) {
		List<HashMap<String, Object>> rmap = null;

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		try {
			conn = ds.getConnection();
			pstmt = conn.prepareStatement(query);

			int index = 1;
			if (params != null) {
				for (Object p : params) {
					pstmt.setObject(index, p);
					index++;
				}
			}

			System.out.println(query + " " + Arrays.asList(params));
			rset = pstmt.executeQuery();

			ResultSetMetaData md = rset.getMetaData();
			int columns = md.getColumnCount();
			int init = 0;
			while (rset.next()) {
				if (init == 0) {
					rmap = new ArrayList<HashMap<String, Object>>();
					init++;
				}

				HashMap<String, Object> map = null;
				map = new HashMap<String, Object>(columns);
				for (int i = 1; i <= columns; ++i) {
					map.put(md.getColumnName(i), rset.getObject(i));
				}

				if (rmap != null)
					rmap.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rset != null)
				try {
					rset.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			if (pstmt != null)
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			if (conn != null)
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}

		return rmap;
	}

	public void execute(String query, List<Object> params) {

		try (Connection conn = ds.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(query);) {

			int index = 1;
			if (params != null) {
				for (Object p : params) {
					pstmt.setObject(index, p);
					index++;
				}
			}

			System.out.println(query + " " + Arrays.asList(params));
			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int executeUpdate(String query, List<Object> params) {

		try (Connection conn = ds.getConnection();
				PreparedStatement pstmt = conn.prepareStatement(query);) {

			int index = 1;
			if (params != null) {
				for (Object p : params) {
					pstmt.setObject(index, p);
					index++;
				}
			}

			System.out.println(query + " " + Arrays.asList(params));
			return pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	public void initPool() {
		String dbhost = "192.168.11.229";
		String dbport = "3306";
		String database = "event_log";
		String dbuser = "root";
		String dbpass = "qwe123";
		String jdbcUrl = "jdbc:mariadb://" + dbhost + ":" + dbport + "/" + database
				+ "?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf8mb4";

		config.setJdbcUrl(jdbcUrl);
		config.setUsername(dbuser);
		config.setPassword(dbpass);
		config.setDriverClassName("org.mariadb.jdbc.Driver");
		config.setConnectionTestQuery("select 1");
		config.addDataSourceProperty("maxPoolSize", 8);
		config.addDataSourceProperty("minPoolSize", 4);
		config.setPoolName("cleanup");
		config.setMaximumPoolSize(8);
		config.setMinimumIdle(4);

		ds = new HikariDataSource(config);
	}

	public void closePool() {
		ds.close();
	}
}