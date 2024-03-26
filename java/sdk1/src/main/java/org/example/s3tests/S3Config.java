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
import java.io.FileReader;

import org.example.Data.UserData;
import org.ini4j.Ini;

public class S3Config
{
	// private final String STR_FILENAME = "config.ini";
	private final String STR_FILENAME = "s3tests-229.ini";
	// private final String STR_FILENAME = "s3tests-227.ini";
	// private final String STR_FILENAME = "s3tests_gw.ini";
	// private final String STR_FILENAME = "s3tests_ksan2.ini";
	// private final String STR_FILENAME = "awstests.ini";
	//////////////////////////////SIGNATURE VERSION////////////////////////////////////
	public static final String STR_SIGNATURE_VERSION_V2 = "S3SignerType";
	public static final String STR_SIGNATURE_VERSION_V4 = "AWSS3V4SignerType";

	/////////////////////////////////////S3///////////////////////////////////////////
	private static final String STR_S3 = "S3";
	private static final String STR_URL = "URL";
	private static final String STR_PORT = "Port";
	private static final String STR_SSL_PORT = "SSLPort";
	private static final String STR_SIGNATURE_VERSION = "SignatureVersion";
	private static final String STR_IS_SECURE = "IsSecure";
	private static final String STR_REGION = "RegionName";

	/////////////////////////////////////Fixtures///////////////////////////////////////////
	private static final String STR_FIXTURES = "Fixtures";
	private static final String STR_BUCKET_PREFIX = "BucketPrefix";
	private static final String STR_BUCKET_DELETE = "NotDelete";
	/////////////////////////////////////User Data///////////////////////////////////////////
	private static final String STR_MAIN_USER = "Main User";
	private static final String STR_ALT_USER = "Alt User";

	private static final String STR_DISPLAY_NAME = "DisplayName";
	private static final String STR_USER_ID = "UserID";
	private static final String STR_EMAIL = "Email";
	private static final String STR_ACCESS_KEY = "AccessKey";
	private static final String STR_SECRET_KEY = "SecretKey";
	private static final String STR_KMS = "KMS";

	/*********************************************************************************************************/
	public final String fileName;
	private final Ini ini = new Ini();
	/*********************************************************************************************************/
	public String url;
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

	public boolean getConfig()
	{
		File file = new File(fileName);
		try {
			ini.load(new FileReader(file));

			url = readKeyToString(STR_S3, STR_URL);
			port = readKeyToInt(STR_S3, STR_PORT);
			sslPort = readKeyToInt(STR_S3, STR_SSL_PORT);
			regionName = readKeyToString(STR_S3, STR_REGION);
			signatureVersion = readKeyToString(STR_S3, STR_SIGNATURE_VERSION);
			isSecure = readKeyToBoolean(STR_S3, STR_IS_SECURE);

			bucketPrefix = readKeyToString(STR_FIXTURES, STR_BUCKET_PREFIX);
			notDelete = readKeyToBoolean(STR_FIXTURES, STR_BUCKET_DELETE);

			mainUser = readUser(STR_MAIN_USER);
			altUser = readUser(STR_ALT_USER);

		} catch (Exception e) {
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
		return url == null || url.length() == 0;
	}

	private UserData readUser(String section)
	{
		UserData user = new UserData();

		user.displayName = readKeyToString(section, STR_DISPLAY_NAME);
		user.userId 	 = readKeyToString(section, STR_USER_ID);
		user.email 		 = readKeyToString(section, STR_EMAIL);
		user.accessKey 	 = readKeyToString(section, STR_ACCESS_KEY);
		user.secretKey 	 = readKeyToString(section, STR_SECRET_KEY);
		user.kms 		 = readKeyToString(section, STR_KMS);

		return user;
	}

	private String readKeyToString(String section, String key) { return ini.get(section, key); }
	private int readKeyToInt(String section, String key) { return Integer.parseInt(ini.get(section, key)); }
	private boolean readKeyToBoolean(String section, String key) { return Boolean.parseBoolean(ini.get(section, key)); }
}
