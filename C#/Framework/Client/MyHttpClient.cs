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
using s3tests2.Signers;

namespace s3tests2.Client
{
	public class MyHttpClient
	{
		private static readonly string METHOD_DELETE = "DELETE";
		private static readonly string METHOD_POST = "POST";
		private static readonly string METHOD_GET = "GET";
		private static readonly string METHOD_LIST = "GET";
		private static readonly string METHOD_PUT = "PUT";

		public static string HEADER_DATA = "NONE";
		public static string HEADER_BACKEND = "x-ifs-admin";
		public static string HEADER_REPLICATION = "x-ifs-replication";
		public static string HEADER_VERSION_ID = "x-ifs-version-id";

		public const int UserDataBlockSize = 65536;

		private string URL;
		private string AccessKey;
		private string SecretKey;

		public MyHttpClient(string URL, string AccessKey, string SecretKey)
		{
			this.URL = URL;
			this.AccessKey = AccessKey;
			this.SecretKey = SecretKey;
		}

		public MyResult PutObject(string Key, string Content, string ContentType = "text/plain")
		{
			byte[] contentHash = AWS4SignerBase.CanonicalRequestHashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(Content));
			String ContentHashString = AWS4SignerBase.ToHexString(contentHash, true);

			var Headers = new Dictionary<string, string>()
			{
				{AWS4SignerBase.X_Amz_Content_SHA256, ContentHashString},
				{"content-length", $"{Content.Length}"},
				{"content-type", ContentType}
			};

			var URI = new Uri($"{URL}/{Key}");

			var Signer = new AWS4SignerForAuthorizationHeader { EndpointUri = URI, HttpMethod = "PUT", Service = "s3", Region = "us-west-2" };

			var Authorization = Signer.ComputeSignature(Headers, "", ContentHashString, AccessKey, SecretKey);

			// express authorization for this as a header
			Headers.Add("Authorization", Authorization);

			// make the call to Amazon S3

			return Put(URI, Headers, Content);
		}
		public MyResult PutObjectChunked(string Key, string Content, string ContentType = "text/plain")
		{
			var Headers = new Dictionary<string, string>
			{
				{AWS4SignerBase.X_Amz_Content_SHA256, AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256},
				{"content-encoding", "aws-chunked"},
				{"content-type", "text/plain"},
				{AWS4SignerBase.X_Amz_Decoded_Content_Length, Content.Length.ToString()}
			};

			var URI = new Uri($"{URL}/{Key}");

			var Signer = new AWS4SignerForChunkedUpload { EndpointUri = URI, HttpMethod = "PUT", Service = "s3", Region = "us-west-2" };
			var TotalLength = Signer.CalculateChunkedContentLength(Content.Length, UserDataBlockSize);

			var Authorization = Signer.ComputeSignature(Headers, "", AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256, AccessKey, SecretKey);
			Headers.Add("Authorization", Authorization);

			try
			{

				var Request = HttpHelpers.ConstructWebRequest(URI, "PUT", Headers);
				var buffer = new byte[UserDataBlockSize];
				var requestStream = Request.GetRequestStream();
				using (var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(Content)))
				{
					var bytesRead = 0;
					while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
					{
						// process into a chunk
						var chunk = Signer.ConstructSignedChunk(bytesRead, buffer);

						// send the chunk
						requestStream.Write(chunk, 0, chunk.Length);
					}

					// last step is to send a signed zero-length chunk to complete the upload
					var FinalChunk = Signer.ConstructSignedChunk(0, buffer);
					requestStream.Write(FinalChunk, 0, FinalChunk.Length);

					using (var Response = (HttpWebResponse)Request.GetResponse())
					{
						var Msg = HttpHelpers.ReadResponseBody(Response);
						return new MyResult() { StatusCode = Response.StatusCode, Message = Msg };
					}
				}
			}
			catch (WebException ex) { throw GetError(ex); }
		}

		public MyResult GetObject(string Key, out string Content)
		{
			var Headers = new Dictionary<string, string>()
			{
				{AWS4SignerBase.X_Amz_Content_SHA256, AWS4SignerBase.EMPTY_BODY_SHA256},
				{"content-length", "0"},
				{"content-type", "text/plain"}
			};

			var URI = new Uri($"{URL}/{Key}");

			var Signer = new AWS4SignerForAuthorizationHeader { EndpointUri = URI, HttpMethod = "GET", Service = "s3", Region = "us-west-2" };

			var Authorization = Signer.ComputeSignature(Headers, "", AWS4SignerBase.EMPTY_BODY_SHA256, AccessKey, SecretKey);

			// express authorization for this as a header
			Headers.Add("Authorization", Authorization);

			// make the call to Amazon S3

			Content = Get(URI, Headers);
			return new MyResult() { StatusCode = HttpStatusCode.OK };
		}

		#region Utility

		/// <summary>
		/// Makes a Post request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static MyResult Post(Uri Endpoint, IDictionary<string, string> Headers, string RequestBody)
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_POST, Headers);

				if (!string.IsNullOrEmpty(RequestBody))
				{
					var buffer = new byte[8192]; // arbitrary buffer size
					var requestStream = request.GetRequestStream();
					using (var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(RequestBody)))
					{
						var bytesRead = 0;
						while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0) requestStream.Write(buffer, 0, bytesRead);
					}
				}

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					return new MyResult()
					{
						StatusCode = Response.StatusCode
					};
				}
			}
			catch (WebException ex) { throw GetError(ex); }
		}

		/// <summary>
		/// Makes a Put request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static MyResult Put(Uri Endpoint, IDictionary<string, string> Headers, string RequestBody)
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_PUT, Headers);

				if (string.IsNullOrEmpty(RequestBody)) throw new ArgumentException("Request Body is null or empty");

				var buffer = new byte[8192]; // arbitrary buffer size
				var requestStream = request.GetRequestStream();
				using (var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(RequestBody)))
				{
					var bytesRead = 0;
					while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) > 0)
					{
						requestStream.Write(buffer, 0, bytesRead);
					}
				}

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					return new MyResult() { StatusCode = Response.StatusCode };
					// if (Response.StatusCode == HttpStatusCode.OK)
					// {
					// 	var ResponseBody = ReadResponseBody(Response);
					// 	if (!string.IsNullOrEmpty(ResponseBody)) return ResponseBody;
					// }
					// else
					// 	throw new Exception($"HTTP call failed, status code: {Response.StatusCode}");
				}
			}
			catch (WebException ex) { throw GetError(ex); }
		}

		/// <summary>
		/// Makes a Get request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static T Get<T>(Uri Endpoint, IDictionary<string, string> Headers) where T : class
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_GET, Headers);

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					if (Response.StatusCode != HttpStatusCode.OK)
						throw new InvalidOperationException($"HTTP call failed, status code: {Response.StatusCode}");

					using (var ResponseStream = Response.GetResponseStream())
					{
						if (ResponseStream != null)
						{
							using (var reader = new StreamReader(ResponseStream))
							{
								var serializer = new XmlSerializer(typeof(T));
								return serializer.Deserialize(reader) as T;
							}
						}
					}
				}
			}
			catch (WebException ex) { throw GetError(ex); }
			return null;
		}
		/// <summary>
		/// Makes a Get request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static string Get(Uri Endpoint, IDictionary<string, string> Headers)
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_GET, Headers);

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					if (Response.StatusCode != HttpStatusCode.OK)
						throw new InvalidOperationException($"HTTP call failed, status code: {Response.StatusCode}");

					using (var ResponseStream = Response.GetResponseStream())
					{
						if (ResponseStream != null)
						{
							using (var reader = new StreamReader(ResponseStream))
							{
								return reader.ReadToEnd();
							}
						}
					}
				}
			}
			catch (WebException ex) { throw GetError(ex); }
			return null;
		}

		/// <summary>
		/// Makes a List request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static T List<T>(Uri Endpoint, IDictionary<string, string> Headers) where T : class
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_LIST, Headers);

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					if (Response.StatusCode != HttpStatusCode.OK)
						throw new InvalidOperationException($"HTTP call failed, status code: {Response.StatusCode}");
					// Console.WriteLine(ReadResponseBody(Response));
					using (var ResponseStream = Response.GetResponseStream())
					{
						if (ResponseStream != null)
						{
							using (var reader = new StreamReader(ResponseStream))
							{
								var serializer = new XmlSerializer(typeof(T));
								return serializer.Deserialize(reader) as T;
							}
						}
					}
				}
			}
			catch (WebException ex) { throw GetError(ex); }
			return null;
		}

		/// <summary>
		/// Makes a Delete request to the specified endpoint
		/// </summary>
		/// <param name="Endpoint"></param>
		/// <param name="Headers"></param>
		/// <param name="RequestBody"></param>
		public static void Delete(Uri Endpoint, IDictionary<string, string> Headers)
		{
			try
			{
				var request = ConstructWebRequest(Endpoint, METHOD_DELETE, Headers);

				// Get the response and read any body into a string, then display.
				using (var Response = (HttpWebResponse)request.GetResponse())
				{
					if (Response.StatusCode != HttpStatusCode.NoContent)
						throw new Exception($"HTTP call failed, status code: {Response.StatusCode}");
				}
			}
			catch (WebException ex) { throw GetError(ex); }
		}

		/// <summary>
		/// Construct a HttpWebRequest onto the specified endpoint and populate
		/// the headers.
		/// </summary>
		/// <param name="URI">The endpoint to call</param>
		/// <param name="httpMethod">GET, PUT etc</param>
		/// <param name="headers">The set of headers to apply to the request</param>
		/// <returns>Initialized HttpWebRequest instance</returns>
		public static HttpWebRequest ConstructWebRequest(Uri URI, string httpMethod, IDictionary<string, string> headers)
		{
			var request = (HttpWebRequest)WebRequest.Create(URI);
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


		/// <summary>
		/// Reads the response data from the service call, if any
		/// </summary>
		/// <param name="response"> The response instance obtained from the previous request </param>
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

		public static S3Exception GetError(WebException ex)
		{
			using (var Response = ex.Response as HttpWebResponse)
			{
				var xs = new XmlSerializer(typeof(ErrorResponse));
				var Result = (ErrorResponse)xs.Deserialize(Response.GetResponseStream());
				if (Result == null)
					return new S3Exception(new ErrorResponse() { Message = $"HTTP call failed. status code '{Response.StatusCode}'" }, ex.InnerException);
				else
					return new S3Exception(Result, ex.InnerException);

			}
		}
		#endregion
	}
}