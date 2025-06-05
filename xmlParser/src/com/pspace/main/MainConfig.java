package com.pspace.main;

import com.pspace.jenkins.DbConfig;
import com.pspace.jenkins.JenkinsConfig;
import lombok.Setter;
import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

@Setter
@Getter
public class MainConfig {
	private DbConfig db;
	private JenkinsConfig jenkins;

	public static MainConfig load(String configFile) throws IOException {
		try (InputStream inputStream = new FileInputStream(configFile)) {
			return new Yaml().loadAs(inputStream, MainConfig.class);
		}
	}
}