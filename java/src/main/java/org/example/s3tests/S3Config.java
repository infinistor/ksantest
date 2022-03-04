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
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

public class S3Config
{
    /////////////////////////////////////Default///////////////////////////////////////////
    private final String STR_GLOBAL = "Global";
    private final String STR_URL = "URL";
    private final String STR_PORT = "Port";
    private final String STR_SIGNATUREVERSION = "SignatureVersion";
    private final String STR_ISSECURE = "IsSecure";
    private final String STR_FILENAME = "awstests.ini";
    // private final String STR_FILENAME = "s3tests_227.ini";
 
    /////////////////////////////////////Fixtures///////////////////////////////////////////
    private final String STR_FIXTURES = "Fixtures";
    private final String STR_BUCKETPREFIX = "BucketPrefix";
    /////////////////////////////////////User Data///////////////////////////////////////////
    private final String STR_MAINUSER = "Main User";
    private final String STR_ALTUSER = "Alt User";

    private final String STR_DISPLAYNAME = "DisplayName";
    private final String STR_USERID = "UserID";
    private final String STR_EMAIL = "Email";
    private final String STR_ACCESSKEY = "AccessKey";
    private final String STR_SECRETKEY = "SecretKey";
    private final String STR_APINAME = "APIName";
    private final String STR_KMS = "KMS";

    /*********************************************************************************************************/
    public final String FileName;
    private final Ini ini = new Ini();
    /*********************************************************************************************************/
    public String URL;
    public int Port;
    public String SignatureVersion;
    public boolean IsSecure;
    public String BucketPrefix;
    public UserData MainUser;
    public UserData AltUser;
    
    public S3Config(String _FileName)
    {
    	if(_FileName == null) FileName = STR_FILENAME;
    	else if(_FileName.isBlank()) FileName = STR_FILENAME;
    	else                 	FileName = _FileName;
    }
    
    public boolean GetConfig()
    {
    	File file = new File(FileName);
    	try {
			ini.load(new FileReader(file));
		   	
			URL = ReadKeyToString(STR_GLOBAL, STR_URL);
			Port = ReadKeyToInt(STR_GLOBAL, STR_PORT);
            SignatureVersion = ReadKeyToString(STR_GLOBAL, STR_SIGNATUREVERSION);
            IsSecure = ReadKeyToBoolean(STR_GLOBAL, STR_ISSECURE);

            BucketPrefix = ReadKeyToString(STR_FIXTURES, STR_BUCKETPREFIX);

            MainUser = ReadUser(STR_MAINUSER);
            AltUser = ReadUser(STR_ALTUSER);
			
		} catch (InvalidFileFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return true;
    }
    
    private String ReadKeyToString(String Section, String Key)
    {
    	return ini.get(Section, Key);
    }
    private int ReadKeyToInt(String Section, String Key)
    {
    	return Integer.parseInt(ini.get(Section, Key));
    }
    private boolean ReadKeyToBoolean(String Section, String Key)
    {
    	return Boolean.parseBoolean(ini.get(Section, Key));
    }
    private UserData ReadUser(String Section)
    {
    	UserData Item = new UserData();
    	
        Item.DisplayName = ReadKeyToString(Section, STR_DISPLAYNAME);
        Item.UserID 	 = ReadKeyToString(Section, STR_USERID);
        Item.Email 		 = ReadKeyToString(Section, STR_EMAIL);
        Item.AccessKey 	 = ReadKeyToString(Section, STR_ACCESSKEY);
        Item.SecretKey 	 = ReadKeyToString(Section, STR_SECRETKEY);
        Item.APIName 	 = ReadKeyToString(Section, STR_APINAME);
        Item.KMS 		 = ReadKeyToString(Section, STR_KMS);
        
        return Item;
    }
}
