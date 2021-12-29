package org.example.s3tests;

public class UserData {
    public String DisplayName ;
    public String UserID      ;
    public String Email       ;
    public String AccessKey   ;
    public String SecretKey   ;
    public String APIName     ;
    public String KMS         ;
 
    public UserData()
    {
        Init();
    }

    public void Init()
    {
        DisplayName = "";
        UserID      = "";
        Email       = "";
        AccessKey   = "";
        SecretKey   = "";
        APIName     = "";
        KMS         = "";
    }

    public boolean IsEmpty()
    {
    	
        if (!DisplayName.isBlank()) return true;
        if (!UserID     .isBlank()) return true;
        if (!Email      .isBlank()) return true;
        if (!AccessKey  .isBlank()) return true;
        if (!SecretKey  .isBlank()) return true;
        if (!APIName    .isBlank()) return true;
        return false;
    }
}
