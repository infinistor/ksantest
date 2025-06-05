package com.pspace.main;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pspace.jenkins.DBManager;
import com.pspace.jenkins.PlatformInfo;
import com.pspace.jenkins.XmlParser;

import java.io.File;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		Options options = new Options();

		options.addOption("h", "help", false, "Program Usage Manual");
		options.addOption("f", "file", true, "*.xml file name");
		options.addOption("b", "build", true, "Input Build Number");
		options.addOption("c", "config", true, "Config file");
		options.addOption("t", "table", true, "Table name");

		// 플랫폼 관련 옵션 추가
		options.addOption("p", "platform", true, "Platform type (ifs3.0b, ifs3.0, ifs3.2, ksan)");
		options.addOption("o", "os", true, "OS type (el7, r8)");
		options.addOption("g", "gateway", true, "Gateway type (s3gw, proxy, -)");
		options.addOption("e", "env", true, "Environment type. nohap, hap");
		options.addOption("r", "trigger", true, "Trigger type (auto, manual)");

		var parser = new DefaultParser();
		var formatter = new HelpFormatter();

		try {
			MainConfig config;

			var cmd = parser.parse(options, args);
			if (cmd.hasOption("h")) {
				help(formatter, options);
				return;
			}

			if (cmd.hasOption("c")) {
				config = MainConfig.load(cmd.getOptionValue("c"));
			} else {
				config = MainConfig.load("config.yml");
			}

			String fileName = cmd.getOptionValue("f");
			String tableName = cmd.getOptionValue("t");
			int buildNumber = 0;

			if (cmd.hasOption("b")) {
				buildNumber = Integer.parseInt(cmd.getOptionValue("b"));
			}

			// 필수 파라미터 검증
			if (fileName == null || fileName.isEmpty()) {
				log.error("FileName is empty!");
				return;
			}

			// 플랫폼 정보 검증
			String platform = cmd.getOptionValue("p");
			String os = cmd.getOptionValue("o");
			String gateway = cmd.getOptionValue("g");
			String envType = cmd.getOptionValue("e");
			String triggerType = cmd.getOptionValue("r");

			if (StringUtils.isAnyBlank(platform, os, gateway, envType, triggerType)) {
				log.error("Platform information is incomplete! Required: platform, os, gateway, env, trigger");
				help(formatter, options);
				return;
			}

			var file = new File(fileName);
			if (!file.exists()) {
				log.error("Error: File does not exist!");
				return;
			}

			var xmlParser = new XmlParser(fileName);
			if (!xmlParser.read()) {
				log.error("Failed to parse XML file!");
				return;
			}

			var platformInfo = new PlatformInfo(platform, os, gateway, envType, triggerType);
			var db = new DBManager(config.getDb(), buildNumber, xmlParser.getTestSuite(), tableName, platformInfo);
			if (!db.insert()) {
				log.error("Failed to insert data into database!");
				return;
			}

			log.info("Successfully processed test results");

		} catch (ParseException e) {
			log.error("Failed to parse command line arguments", e);
			help(formatter, options);
		} catch (Exception e) {
			log.error("Unexpected error occurred", e);
		}
	}

	private static void help(HelpFormatter formatter, Options options) {
		formatter.printHelp("xml-report-parser", options);
	}
}
