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
    // private final String STR_FILENAME = "awstests.ini";
    private final String STR_FILENAME = "s3tests.ini";
 
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
