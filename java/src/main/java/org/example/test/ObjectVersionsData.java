package org.example.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.Tag;

public class ObjectVersionsData {
    public String BucketName;
    public String Key;
    public List<Tag> TagSet;
    public Map<String, String> VersionsBody;

    public ObjectVersionsData()
    {
        Init();
    }
    public ObjectVersionsData(String BucketName, String Key, List<Tag> TagSet)
    {
        this.BucketName = BucketName;
        this.Key = Key;
        this.TagSet = TagSet;
        VersionsBody = new HashMap<String, String>();
    }

    public void Init()
    {
        BucketName = "";
        Key = "";
        TagSet = new ArrayList<Tag>();
        VersionsBody = new HashMap<String, String>();
    }

    public void addVersion(String VersionId, String Body){
        VersionsBody.put(VersionId, Body);
    }
}