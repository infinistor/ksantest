package org.example.s3tests;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;

public class BucketResourceData
{
	public AmazonS3 Client;
	public String BucketName;
 
    public BucketResourceData(AmazonS3 _Client, String _BucketName)
    {
        Client = _Client;
        BucketName = _BucketName;
    }

    public PutObjectResult PutObject(String Key, String Body)
    {
        if (Client == null) return null;
        if (BucketName.isBlank()) return null;

        return Client.putObject(BucketName, Key, Body);
    }

    static public boolean IsEmpty(BucketResourceData Data)
    {
        if (Data == null) return true;
        if (Data.Client == null) return true;
        if (Data.BucketName.isBlank()) return true;

        return false;
    }
}