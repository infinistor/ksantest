﻿/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
namespace ReplicationTest
{
	public class DBConfig(string host, int port, string dbName, string tableName, string userName, string password)
	{
		/// <summary> 호스트 이름 </summary>
		public readonly string Host = host;

		/// <summary> 포트 번호 </summary>
		public readonly int Port = port;

		/// <summary> 데이터베이스 이름 </summary>
		public readonly string DBName = dbName;

		/// <summary> 테이블 이름 </summary>
		public readonly string TableName = tableName;

		/// <summary> 사용자 이름 </summary>
		public readonly string UserName = userName;

		/// <summary> 비밀번호 </summary>
		public readonly string Password = password;

		public string ConnectionString
		=> string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4}", Host, Port, DBName, UserName, Password);
	}
}
