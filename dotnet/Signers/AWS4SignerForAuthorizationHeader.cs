using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace s3tests.Signers
{
	/// <summary>
	/// Sample AWS4 signer demonstrating how to sign requests to Amazon S3
	/// using an 'Authorization' header.
	/// </summary>
	public class AWS4SignerForAuthorizationHeader : AWS4SignerBase
	{
		/// <summary>
		/// Computes an AWS4 signature for a request, ready for inclusion as an 
		/// 'Authorization' header.
		/// </summary>
		/// <param name="Headers">
		/// The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
		/// </param>
		/// <param name="QueryParameters">
		/// Any query parameters that will be added to the endpoint. The parameters 
		/// should be specified in canonical format.
		/// </param>
		/// <param name="BodyHash">
		/// Precomputed SHA256 hash of the request body content; this value should also
		/// be set as the header 'X-Amz-Content-SHA256' for non-streaming uploads.
		/// </param>
		/// <param name="AccessKey">
		/// The user's AWS Access Key.
		/// </param>
		/// <param name="SecretKey">
		/// The user's AWS Secret Key.
		/// </param>
		/// <returns>
		/// The computed authorization string for the request. This value needs to be set as the 
		/// header 'Authorization' on the subsequent HTTP request.
		/// </returns>
		public string ComputeSignature(IDictionary<string, string> Headers, string QueryParameters, string BodyHash, string AccessKey, string SecretKey)
		{
			// first get the date and time for the subsequent request, and convert to ISO 8601 format
			// for use in signature generation
			var RequestDateTime = DateTime.UtcNow;
			var DateTimeStamp = RequestDateTime.ToString(ISO8601BasicFormat, CultureInfo.InvariantCulture);

			// update the headers with required 'x-amz-date' and 'host' values
			Headers.Add(X_Amz_Date, DateTimeStamp);

			var HostHeader = EndpointUri.Host;
			if (!EndpointUri.IsDefaultPort)
				HostHeader += ":" + EndpointUri.Port;
			Headers.Add("Host", HostHeader);

			// canonicalize the headers; we need the set of header names as well as the
			// names and values to go into the signature process
			var CanonicalizedHeaderNames = CanonicalizeHeaderNames(Headers);
			var CanonicalizedHeaders = CanonicalizeHeaders(Headers);

			// if any query string parameters have been supplied, canonicalize them
			// (note this sample assumes any required url encoding has been done already)
			var CanonicalizedQueryParameters = string.Empty;
			if (!string.IsNullOrEmpty(QueryParameters))
			{
				var ParamDictionary = QueryParameters.Split("&").Select(p => p.Split("="))
										.ToDictionary(nameval => nameval[0],
										nameval => nameval.Length > 1 ? nameval[1] : "");

				var sb = new StringBuilder();
				var ParamKeys = new List<string>(ParamDictionary.Keys);
				ParamKeys.Sort(StringComparer.Ordinal);
				foreach (var p in ParamKeys)
				{
					if (sb.Length > 0) sb.Append("&");
					sb.AppendFormat("{0}={1}", p, ParamDictionary[p]);
				}

				CanonicalizedQueryParameters = sb.ToString();
			}

			// canonicalize the various components of the request
			var CanonicalRequest = CanonicalizeRequest(EndpointUri, HttpMethod, CanonicalizedQueryParameters, CanonicalizedHeaderNames, CanonicalizedHeaders, BodyHash);
			// Console.WriteLine("\nCanonicalRequest:\n{0}", canonicalRequest);

			// generate a hash of the canonical request, to go into signature computation
			var CanonicalRequestHashBytes = CanonicalRequestHashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(CanonicalRequest));

			// construct the string to be signed
			var StringToSign = new StringBuilder();

			var DateStamp = RequestDateTime.ToString(DateStringFormat, CultureInfo.InvariantCulture);
			var Scope = $"{DateStamp}/{Region}/{Service}/{TERMINATOR}";

			StringToSign.Append($"{SCHEME}-{ALGORITHM}\n{DateTimeStamp}\n{Scope}\n");
			StringToSign.Append(ToHexString(CanonicalRequestHashBytes, true));

			// Console.WriteLine("\nStringToSign:\n{0}", stringToSign);

			// compute the signing key
			var kha = KeyedHashAlgorithm.Create(HMACSHA256);
			kha.Key = DeriveSigningKey(HMACSHA256, SecretKey, Region, DateStamp, Service);

			// compute the AWS4 signature and return it
			var signature = kha.ComputeHash(Encoding.UTF8.GetBytes(StringToSign.ToString()));
			var signatureString = ToHexString(signature, true);
			// Console.WriteLine("\nSignature:\n{0}", signatureString);

			var authString = new StringBuilder();
			authString.Append($"{SCHEME}-{ALGORITHM} ");
			authString.Append($"Credential={AccessKey}/{Scope}, ");
			authString.Append($"SignedHeaders={CanonicalizedHeaderNames}, ");
			authString.Append($"Signature={signatureString}");

			var authorization = authString.ToString();
			// Console.WriteLine("\nAuthorization:\n{0}", authorization);

			return authorization;
		}
	}
}
