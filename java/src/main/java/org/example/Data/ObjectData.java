/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
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

	private ObjectData(Builder builder) {
		this.bucketName = builder.bucketName;
		this.key = builder.key;
		this.displayName = builder.displayName;
		this.id = builder.id;
		this.eTag = builder.eTag;
		this.lastModified = builder.lastModified;
		this.contentLength = builder.contentLength;
		this.versionId = builder.versionId;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private String bucketName;
		private String key;
		private String displayName;
		private String id;
		private String eTag;
		private Date lastModified;
		private long contentLength;
		private String versionId;

		public Builder bucketName(String bucketName) {
			this.bucketName = bucketName;
			return this;
		}

		public Builder key(String key) {
			this.key = key;
			return this;
		}

		public Builder displayName(String displayName) {
			this.displayName = displayName;
			return this;
		}

		public Builder id(String id) {
			this.id = id;
			return this;
		}

		public Builder eTag(String eTag) {
			this.eTag = eTag;
			return this;
		}

		public Builder lastModified(Date lastModified) {
			this.lastModified = lastModified;
			return this;
		}

		public Builder contentLength(long contentLength) {
			this.contentLength = contentLength;
			return this;
		}

		public Builder versionId(String versionId) {
			this.versionId = versionId;
			return this;
		}

		public ObjectData build() {
			return new ObjectData(this);
		}
	}
}