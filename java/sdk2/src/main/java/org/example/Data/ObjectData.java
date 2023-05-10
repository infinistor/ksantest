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
package org.example.Data;

import java.util.Date;

public class ObjectData {
	public String bucketName;
	public String key;
	public String displayName;
	public String id;
	public String eTag;
	public Date lastModified;
	public long contentLength;
	public String versionId;

	public ObjectData()
	{
		Init();
	}

	public void Init()
	{
		bucketName = null;
		key = null;
		displayName = null;
		id = null;
		eTag = null;
		lastModified = null;
		contentLength = -1;
		versionId = null;
	}


	public ObjectData withBucketName(String bucketName) {
		setBucketName(bucketName);
		return this;
	}
	public ObjectData withKey(String key) {
		setKey(key);
		return this;
	}
	public ObjectData withDisplayName(String displayName) {
		setDisplayName(displayName);
		return this;
	}
	public ObjectData withID(String id) {
		setId(id);
		return this;
	}
	public ObjectData withETag(String eTag) {
		setETag(eTag);
		return this;
	}
	public ObjectData withLastModified(Date lastModified) {
		setLastModified(lastModified);
		return this;
	}
	public ObjectData withContentLength(long contentLength) {
		setContentLength(contentLength);
		return this;
	}
	public ObjectData withVersionId(String versionId) {
		setVersionId(versionId);
		return this;
	}


	public void setBucketName(String bucketName) { this.bucketName = bucketName; }
	public String getBucketName() { return bucketName; }
	public void setKey(String key) { this.key = key; }
	public String getKey() { return key; }
	public void setDisplayName(String displayName) { this.displayName = displayName; }
	public String getDisplayName() { return displayName; }
	public void setId(String id) { this.id = id; }
	public String getId() { return id; }
	public void setETag(String eTag) { this.eTag = eTag; }
	public String getETag() { return eTag; }
	public void setLastModified(Date lastModified) { this.lastModified = lastModified; }
	public Date getLastModified() { return lastModified; }
	public void setContentLength(long contentLength) { this.contentLength = contentLength; }
	public long getContentLength() { return contentLength; }
	public void setVersionId(String versionId) { this.versionId = versionId; }
	public String getVersionId() { return versionId; }
}