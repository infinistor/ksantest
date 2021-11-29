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

public class FormFile {
	String Name;
	String ContentType;
	String Body;
	 
	public FormFile(String Name, String ContentType, String Body)
	{
		this.Name = Name;
		this.ContentType = ContentType;
		this.Body = Body;
	}
	
	public String getName() {return Name;}
	public String getContentType() {return ContentType;}
	public String getBody() {return Body;}
	
	public void setName(String Name) {this.Name= Name;}
	public void setContentType(String ContentType) {this.ContentType= ContentType;}
	public void setBody(String Body) {this.Body= Body;}
}