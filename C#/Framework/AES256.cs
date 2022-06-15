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
using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace s3tests2
{
	class AES256
	{
		private const int KEY_SIZE = 32;
		private const int VI_SIZE = 16;
		private static readonly char[] TEXT = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

		public readonly string Key;
		public readonly byte[] ByteKey;

		public readonly string IV;
		public readonly byte[] ByteIV;

		public readonly RijndaelManaged AES;

		public AES256()
		{
			Key = RendomString(KEY_SIZE);
			ByteKey = Encoding.UTF8.GetBytes(Key);

			IV = RendomString(VI_SIZE);
			ByteIV = Encoding.UTF8.GetBytes(IV);

			AES = new RijndaelManaged
			{
				KeySize = 256,
				Mode = CipherMode.CBC,
				Padding = PaddingMode.PKCS7,
				Key = ByteKey,
				IV = ByteIV
			};
		}

		public string AESEncrypt(string Input)
		{
			var encrypt = AES.CreateEncryptor(AES.Key, AES.IV);
			byte[] buf = null;
			using (var ms = new MemoryStream())
			{
				using (var cs = new CryptoStream(ms, encrypt, CryptoStreamMode.Write))
				{
					byte[] xXml = Encoding.UTF8.GetBytes(Input);
					cs.Write(xXml, 0, xXml.Length);
				}
				buf = ms.ToArray();
			}
			string Output = Convert.ToBase64String(buf);
			return Output;
		}

		public string AESDecrypt(string input)
		{
			var decrypt = AES.CreateDecryptor();
			byte[] buf = null;
			using (var ms = new MemoryStream())
			{
				using (var cs = new CryptoStream(ms, decrypt, CryptoStreamMode.Write))
				{
					byte[] xXml = Convert.FromBase64String(input);
					cs.Write(xXml, 0, xXml.Length);
				}
				buf = ms.ToArray();
			}
			string Output = Encoding.UTF8.GetString(buf);
			return Output;
		}

		private string RendomString(int Length)
		{
			Random rand = new Random();
			var chars = Enumerable.Range(0, Length).Select(x => TEXT[rand.Next(0, TEXT.Length)]);
			return new string(chars.ToArray());
		}
	}
}
