/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.example.Data.UserData;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

public class S3Config
{
	private final String STR_FILENAME = "config.ini";
	// private final String STR_FILENAME = "s3tests-229.ini";
	// private final String STR_FILENAME = "s3tests-227.ini";
	// private final String STR_FILENAME = "s3tests_gw.ini";
	// private final String STR_FILENAME = "s3tests_ksan.ini";
	// private final String STR_FILENAME = "awstests.ini";
	//////////////////////////////SIGNATUREVERSION////////////////////////////////////
	public final static String STR_SIGNATURE_VERSION_V2 = "S3SignerType";
	public final static String STR_SIGNATURE_VERSION_V4 = "AWSS3V4SignerType";

	/////////////////////////////////////S3///////////////////////////////////////////
	private final String STR_S3 = "S3";
	private final String STR_URL = "URL";
	private final String STR_PORT = "Port";
	private final String STR_SSL_PORT = "SSLPort";
	private final String STR_SIGNATURE_VERSION = "SignatureVersion";
	private final String STR_IS_SECURE = "IsSecure";
	private final String STR_REGION = "RegionName";

	/////////////////////////////////////Fixtures///////////////////////////////////////////
	private final String STR_FIXTURES = "Fixtures";
	private final String STR_BUCKET_PREFIX = "BucketPrefix";
	private final String STR_BUCKET_DELETE = "NotDelete";
	/////////////////////////////////////User Data///////////////////////////////////////////
	private final String STR_MAIN_USER = "Main User";
	private final String STR_ALT_USER = "Alt User";

	private final String STR_DISPLAY_NAME = "DisplayName";
	private final String STR_USER_ID = "UserID";
	private final String STR_EMAIL = "Email";
	private final String STR_ACCESS_KEY = "AccessKey";
	private final String STR_SECRET_KEY = "SecretKey";
	private final String STR_KMS = "KMS";

	/*********************************************************************************************************/
	public final String fileName;
	private final Ini ini = new Ini();
	/*********************************************************************************************************/
	public String URL;
	public int port;
	public int sslPort;
	public String regionName;
	public String signatureVersion;
	public boolean isSecure;
	public String bucketPrefix;
	public boolean notDelete;
	public UserData mainUser;
	public UserData altUser;

	public S3Config(String fileName)
	{
		if(fileName == null) this.fileName = STR_FILENAME;
		else if(fileName.isBlank()) this.fileName = STR_FILENAME;
		else this.fileName = fileName;
	}

	public boolean GetConfig()
	{
		File file = new File(fileName);
		try {
			ini.load(new FileReader(file));

			URL = readKeyToString(STR_S3, STR_URL);
			port = readKeyToInt(STR_S3, STR_PORT);
			sslPort = readKeyToInt(STR_S3, STR_SSL_PORT);
			regionName = readKeyToString(STR_S3, STR_REGION);
			signatureVersion = readKeyToString(STR_S3, STR_SIGNATURE_VERSION);
			isSecure = readKeyToBoolean(STR_S3, STR_IS_SECURE);

			bucketPrefix = readKeyToString(STR_FIXTURES, STR_BUCKET_PREFIX);
			notDelete = readKeyToBoolean(STR_FIXTURES, STR_BUCKET_DELETE);

			mainUser = readUser(STR_MAIN_USER);
			altUser = readUser(STR_ALT_USER);

		} catch (InvalidFileFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public String getSignatureVersion()
	{
		if (signatureVersion.equals("2")) return STR_SIGNATURE_VERSION_V2;
		return STR_SIGNATURE_VERSION_V4;
	}
	public boolean isAWS()
	{
		if (URL == null || URL.length() == 0) return true;
		return false;
	}

	private UserData readUser(String Section)
	{
		UserData Item = new UserData();

		Item.displayName = readKeyToString(Section, STR_DISPLAY_NAME);
		Item.userId 	 = readKeyToString(Section, STR_USER_ID);
		Item.email 		 = readKeyToString(Section, STR_EMAIL);
		Item.accessKey 	 = readKeyToString(Section, STR_ACCESS_KEY);
		Item.secretKey 	 = readKeyToString(Section, STR_SECRET_KEY);
		Item.KMS 		 = readKeyToString(Section, STR_KMS);

		return Item;
	}

	private String readKeyToString(String section, String key) { return ini.get(section, key); }
	private int readKeyToInt(String section, String key) { return Integer.parseInt(ini.get(section, key)); }
	private boolean readKeyToBoolean(String section, String key) { return Boolean.parseBoolean(ini.get(section, key)); }
}
