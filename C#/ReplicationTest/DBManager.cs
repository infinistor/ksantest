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
using log4net;
using MySql.Data.MySqlClient;
using System;
using System.Reflection;

namespace ReplicationTest
{
    class DBManager
    {
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public DBInfo DB;

        public MySqlConnection Conn = null;
        public DBManager(DBInfo DB)
        {
            this.DB = DB;
        }

        public bool Connect()
        {
            try
            {
                Conn = new MySqlConnection(DB.GetaccessCode());
                Conn.Open();
                return true;
            }
            catch (Exception e)
            {
                log.Error(e);
                return false;
            }
        }

        public bool Insert(int BuildID, string TestCase, string Result, string Message)
        {
            if (Conn == null) return false;
            if (BuildID == 0) return false;
            try
            {
                string Query = $"insert into replication_test(build_id, test_case, result, message) values({BuildID},'{TestCase}','{Result}','{Message}')";

                MySqlCommand command = new MySqlCommand(Query, Conn);
                if (command.ExecuteNonQuery() != 1)
                {
                    log.ErrorFormat("Insert Failed({0})", Query);
                    return false;
                }
                return true;
            }
            catch (Exception e)
            {
                log.Error(e);
                return false;
            }
        }

        public void Close()
        {
            if (Conn != null)
            {
                Conn.Close();
            }
        }
    }
}
