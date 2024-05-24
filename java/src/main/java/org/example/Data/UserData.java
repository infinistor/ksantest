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
public class UserData {
	public String displayName;
	public String userId;
	public String email;
	public String accessKey;
	public String secretKey;
	public String kms;

	public UserData() {
		displayName = "";
		userId = "";
		email = "";
		accessKey = "";
		secretKey = "";
		kms = "";
	}

	public UserData(String displayName, String userId, String email, String accessKey, String secretKey, String kms) {
		this.displayName = displayName;
		this.userId = userId;
		this.email = email;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.kms = kms;
	}

	public software.amazon.awssdk.services.s3.model.Grantee toGrantee() {
		return software.amazon.awssdk.services.s3.model.Grantee.builder()
			.displayName(displayName)
			.id(userId)
			.emailAddress(email)
			.type("CanonicalUser")
			.build();
	}

	public software.amazon.awssdk.services.s3.model.Owner toOwner() {
		return software.amazon.awssdk.services.s3.model.Owner.builder()
			.displayName(displayName)
			.id(userId)
			.build();
	}
}
