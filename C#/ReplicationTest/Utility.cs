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
using Amazon.S3.Model;
using Amazon.S3;
using log4net;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;

namespace ReplicationTest
{
    class Utility
    {
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly char[] TEXT = "abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray();
        private static readonly char[] TEXT_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

        public static bool CreateDummyFile(string FilePath, int FileSize)
        {
            try
            {
                if (new FileInfo(FilePath).Exists) File.Delete(FilePath);
                FileStream fs = new FileStream(FilePath, FileMode.CreateNew);
                fs.Seek(FileSize, SeekOrigin.Begin);
                fs.WriteByte(0);
                fs.Close();
                return true;
            }
            catch (Exception e)
            {
                log.Error(e);
                return false;
            }
        }

        public static bool CheckFile(string FilePath)
        {
            try
            {
                if (new FileInfo(FilePath).Exists) return true;
            }
            catch (Exception e)
            {
                log.Error(e);
            }
            return false;
        }

        public static string RandomText(int Length)
        {
            Random rand = new Random(Guid.NewGuid().GetHashCode());
            var chars = Enumerable.Range(0, Length).Select(x => TEXT[rand.Next(0, TEXT.Length)]);
            return new string(chars.ToArray());
        }
        public static string RandomTextToLong(int Length)
        {
            Random rand = new Random(Guid.NewGuid().GetHashCode());
            var chars = Enumerable.Range(0, Length).Select(x => TEXT_STRING[rand.Next(0, TEXT_STRING.Length)]);
            return new string(chars.ToArray());
        }

        public static HttpStatusCode GetStatus(AggregateException e)
        {
            if (e.InnerException is AmazonS3Exception e2) return e2.StatusCode;
            return HttpStatusCode.OK;
        }
        public static string GetErrorCode(AggregateException e)
        {
            if (e.InnerException is AmazonS3Exception e2) return e2.ErrorCode;
            return null;
        }
        public static string GetBody(GetObjectResponse Response)
        {
            string Body = string.Empty;
            if (Response != null && Response.ResponseStream != null)
            {
                var Reader = new StreamReader(Response.ResponseStream);
                Body = Reader.ReadToEnd();
                Reader.Close();
            }
            return Body;
        }
    }
}
