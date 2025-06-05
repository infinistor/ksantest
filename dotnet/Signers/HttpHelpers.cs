/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Xml.Serialization;

namespace s3tests.Signers
{
	/// <summary>
	/// Various Http helper routines
	/// </summary>
	public static class HttpHelpers
	{
		/// <summary>
		/// Makes a http request to the specified endpoint
		/// </summary>
		/// <param name="endpointUri"></param>
		/// <param name="httpMethod"></param>
		/// <param name="headers"></param>
		/// <param name="requestBody"></param>
		public static void InvokeHttpRequest(Uri endpointUri, string httpMethod, IDictionary<string, string> headers, string requestBody)
		{
			try
			{
				var request = ConstructWebRequest(endpointUri, httpMethod, headers);

				if (!string.IsNullOrEmpty(requestBody))
				{
					var buffer = new byte[8192]; // arbitrary buffer size
					var requestStream = request.GetRequestStream();
					using (var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(requestBody)))
					{
						var bytesRead = 0;
						while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
						{
							requestStream.Write(buffer, 0, bytesRead);
						}
					}
				}

				CheckResponse(request);
			}
			catch (WebException ex)
			{
				using (var Response = ex.Response as HttpWebResponse)
				{
					var xs = new XmlSerializer(typeof(ErrorResponse));
					var Result = (ErrorResponse)xs.Deserialize(Response.GetResponseStream());
					if (Response != null)
					{
						var errorMsg = ReadResponseBody(Response);
						Console.WriteLine("\n-- HTTP call failed with exception '{0}', status code '{1}'", errorMsg, Response.StatusCode);
					}
					else
						throw new S3Exception(Result, ex.InnerException);
				}
			}
		}

		/// <summary>
		/// Construct a HttpWebRequest onto the specified endpoint and populate
		/// the headers.
		/// </summary>
		/// <param name="endpointUri">The endpoint to call</param>
		/// <param name="httpMethod">GET, PUT etc</param>
		/// <param name="headers">The set of headers to apply to the request</param>
		/// <returns>Initialized HttpWebRequest instance</returns>
		public static HttpWebRequest ConstructWebRequest(Uri endpointUri,
														 string httpMethod,
														 IDictionary<string, string> headers)
		{
			var request = (HttpWebRequest)WebRequest.Create(endpointUri);
			request.Method = httpMethod;

			foreach (var header in headers.Keys)
			{
				// not all headers can be set via the dictionary
				if (header.Equals("host", StringComparison.OrdinalIgnoreCase))
					request.Host = headers[header];
				else if (header.Equals("content-length", StringComparison.OrdinalIgnoreCase))
					request.ContentLength = long.Parse(headers[header]);
				else if (header.Equals("content-type", StringComparison.OrdinalIgnoreCase))
					request.ContentType = headers[header];
				else
					request.Headers.Add(header, headers[header]);
			}

			return request;
		}

		public static void CheckResponse(HttpWebRequest request)
		{
			// Get the response and read any body into a string, then display.
			using (var response = (HttpWebResponse)request.GetResponse())
			{
				if (response.StatusCode == HttpStatusCode.OK)
				{
					Console.WriteLine("\n-- HTTP call succeeded");
					var responseBody = ReadResponseBody(response);
					if (!string.IsNullOrEmpty(responseBody))
					{
						Console.WriteLine("\n-- Response body:");
						Console.WriteLine(responseBody);
					}
				}
				else
					Console.WriteLine("\n-- HTTP call failed, status code: {0}", response.StatusCode);
			}
		}

		/// <summary>
		/// Reads the response data from the service call, if any
		/// </summary>
		/// <param name="response">
		/// The response instance obtained from the previous request
		/// </param>
		/// <returns>The body content of the response</returns>
		public static string ReadResponseBody(HttpWebResponse response)
		{
			if (response == null)
				throw new ArgumentNullException("response", "Value cannot be null");

			// Then, open up a reader to the response and read the contents to a string
			// and return that to the caller.
			string responseBody = string.Empty;
			using (var responseStream = response.GetResponseStream())
			{
				if (responseStream != null)
				{
					using (var reader = new StreamReader(responseStream))
					{
						responseBody = reader.ReadToEnd();
					}
				}
			}
			return responseBody;
		}


		/// <summary>
		/// Helper routine to url encode canonicalized header names and values for safe
		/// inclusion in the presigned url.
		/// </summary>
		/// <param name="data">The string to encode</param>
		/// <param name="isPath">Whether the string is a URL path or not</param>
		/// <returns>The encoded string</returns>
		public static string UrlEncode(string data, bool isPath = false)
		{
			// The Set of accepted and valid Url characters per RFC3986. Characters outside of this set will be encoded.
			const string validUrlCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~";

			var encoded = new StringBuilder(data.Length * 2);
			string unreservedChars = String.Concat(validUrlCharacters, (isPath ? "/:" : ""));

			foreach (char symbol in System.Text.Encoding.UTF8.GetBytes(data))
			{
				if (unreservedChars.IndexOf(symbol) != -1)
					encoded.Append(symbol);
				else
					encoded.Append("%").Append(String.Format("{0:X2}", (int)symbol));
			}

			return encoded.ToString();
		}
	}
}
