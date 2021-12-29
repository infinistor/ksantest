package org.example.s3tests;

import java.util.ArrayList;

import com.amazonaws.services.s3.model.PartETag;

public class MultipartUploadData {
    public String UploadID;
    public ArrayList<PartETag> Parts;
    public String Data;
   
    public MultipartUploadData()
    {
        Init();
    }

    public void Init()
    {
        UploadID = "";
        Data = "";
        Parts = new ArrayList<PartETag>();
    }
}