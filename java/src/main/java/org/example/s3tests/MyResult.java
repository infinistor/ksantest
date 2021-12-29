package org.example.s3tests;

public class MyResult {

    public int StatusCode;
    public String ErrorCode;
    public String Message;
    public String URL;
 
    public MyResult()
    {
        Init();
    }
    public void Init()
    {
        StatusCode = -1;
        ErrorCode = "";
        Message = "";
        URL = "";
    }
}