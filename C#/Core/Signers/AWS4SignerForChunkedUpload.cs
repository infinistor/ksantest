using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace s3tests.Signers
{
	/// <summary>
	/// Sample AWS4 signer demonstrating how to sign 'chunked' uploads
	/// to Amazon S3 using an Authorization header.
	/// </summary>
	public class AWS4SignerForChunkedUpload : AWS4SignerBase
	{
		// SHA256 substitute marker used in place of x-amz-content-sha256 when employing 
		// chunked uploads
		public const string STREAMING_BODY_SHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

		static readonly string CLRF = "\r\n";
		static readonly string CHUNK_STRING_TO_SIGN_PREFIX = "AWS4-HMAC-SHA256-PAYLOAD";
		static readonly string CHUNK_SIGNATURE_HEADER = ";chunk-signature=";
		static readonly int SIGNATURE_LENGTH = 64;
		static byte[] FINAL_CHUNK = new byte[0];

		/// <summary>
		/// Tracks the previously computed signature value; for chunk 0 this will
		/// contain the signature included in the Authorization header. For subsequent
		/// chunks it contains the computed signature of the prior chunk.
		/// </summary>
		public string LastComputedSignature { get; private set; }

		/// <summary>
		/// Date and time of the original signing computation, in ISO 8601 basic format,
		/// reused for each chunk
		/// </summary>
		public string DateTimeStamp { get; private set; }

		/// <summary>
		/// The scope value of the original signing computation, reused for each chunk
		/// </summary>
		public string Scope { get; private set; }

		/// <summary>
		/// The derived signing key used in the original signature computation and
		/// re-used for each chunk
		/// </summary>
		byte[] SigningKey { get; set; }

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
			DateTimeStamp = RequestDateTime.ToString(ISO8601BasicFormat, CultureInfo.InvariantCulture);

			// update the headers with required 'x-amz-date' and 'host' values
			Headers.Add(X_Amz_Date, DateTimeStamp);

			var HostHeader = EndpointUri.Host;
			if (!EndpointUri.IsDefaultPort)
				HostHeader += ":" + EndpointUri.Port;
			Headers.Add("Host", HostHeader);

			// canonicalize the headers; we need the set of header names as well as the
			// names and values to go into the signature process
			var canonicalizedHeaderNames = CanonicalizeHeaderNames(Headers);
			var canonicalizedHeaders = CanonicalizeHeaders(Headers);

			// if any query string parameters have been supplied, canonicalize them
			// (note this sample assumes any required url encoding has been done already)
			var canonicalizedQueryParameters = string.Empty;
			if (!string.IsNullOrEmpty(QueryParameters))
			{
				var paramDictionary = QueryParameters.Split('&').Select(p => p.Split('='))
													 .ToDictionary(nameval => nameval[0],
																   nameval => nameval.Length > 1
																		? nameval[1] : "");

				var sb = new StringBuilder();
				var paramKeys = new List<string>(paramDictionary.Keys);
				paramKeys.Sort(StringComparer.Ordinal);
				foreach (var p in paramKeys)
				{
					if (sb.Length > 0)
						sb.Append("&");
					sb.AppendFormat("{0}={1}", p, paramDictionary[p]);
				}

				canonicalizedQueryParameters = sb.ToString();
			}

			// canonicalize the various components of the request
			var CanonicalRequest = CanonicalizeRequest(EndpointUri, HttpMethod, canonicalizedQueryParameters, canonicalizedHeaderNames, canonicalizedHeaders, BodyHash);
			// Console.WriteLine("\nCanonicalRequest:\n{0}", canonicalRequest);

			// generate a hash of the canonical request, to go into signature computation
			var CanonicalRequestHashBytes = CanonicalRequestHashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(CanonicalRequest));

			// construct the string to be signed
			var StringToSign = new StringBuilder();

			var DateStamp = RequestDateTime.ToString(DateStringFormat, CultureInfo.InvariantCulture);
			Scope = $"{DateStamp}/{Region}/{Service}/{TERMINATOR}";

			StringToSign.Append($"{SCHEME}-{ALGORITHM}\n{DateTimeStamp}\n{Scope}\n");
			StringToSign.Append(ToHexString(CanonicalRequestHashBytes, true));

			// Console.WriteLine("\nStringToSign:\n{0}", stringToSign);

			// compute the signing key
			SigningKey = DeriveSigningKey(HMACSHA256, SecretKey, Region, DateStamp, Service);

			var kha = KeyedHashAlgorithm.Create(HMACSHA256);
			kha.Key = SigningKey;

			// compute the AWS4 signature and return it
			var signature = kha.ComputeHash(Encoding.UTF8.GetBytes(StringToSign.ToString()));
			var signatureString = ToHexString(signature, true);
			// Console.WriteLine("\nSignature:\n{0}", signatureString);

			// cache the computed signature ready for chunk 0 upload
			LastComputedSignature = signatureString;

			var authString = new StringBuilder();
			authString.Append($"{SCHEME}-{ALGORITHM} ");
			authString.Append($"Credential={AccessKey}/{Scope}, ");
			authString.Append($"SignedHeaders={canonicalizedHeaderNames}, ");
			authString.Append($"Signature={signatureString}");

			var authorization = authString.ToString();
			// Console.WriteLine("\nAuthorization:\n{0}", authorization);

			return authorization;
		}

		/// <summary>
		/// Calculates the expanded payload size of our data when it is chunked
		/// </summary>
		/// <param name="originalLength">
		/// The true size of the data payload to be uploaded
		/// </param>
		/// <param name="chunkSize">
		/// The size of each chunk we intend to send; each chunk will be
		/// prefixed with signed header data, expanding the overall size
		/// by a determinable amount
		/// </param>
		/// <returns>
		/// The overall payload size to use as content-length on a chunked upload
		/// </returns>
		public long CalculateChunkedContentLength(long originalLength, long chunkSize)
		{
			if (originalLength <= 0)
				throw new ArgumentOutOfRangeException("originalLength");
			if (chunkSize <= 0)
				throw new ArgumentOutOfRangeException("chunkSize");

			var maxSizeChunks = originalLength / chunkSize;
			var remainingBytes = originalLength % chunkSize;

			var chunkedContentLength = maxSizeChunks * CalculateChunkHeaderLength(chunkSize)
									   + (remainingBytes > 0 ? CalculateChunkHeaderLength(remainingBytes) : 0)
									   + CalculateChunkHeaderLength(0);

			// Console.WriteLine("\nComputed chunked content length for original length {0} bytes, chunk size {1}KB is {2} bytes", originalLength, chunkSize / 1024, chunkedContentLength);
			return chunkedContentLength;
		}

		/// <summary>
		/// Returns the size of a chunk header, which only varies depending
		/// on the selected chunk size
		/// </summary>
		/// <param name="chunkSize">
		/// The intended size of each chunk; this is placed into the chunk 
		/// header
		/// </param>
		/// <returns>
		/// The overall size of the header that will prefix the user data in 
		/// each chunk
		/// </returns>
		static long CalculateChunkHeaderLength(long chunkSize)
		{
			return chunkSize.ToString("X").Length
					+ CHUNK_SIGNATURE_HEADER.Length
					+ SIGNATURE_LENGTH
					+ CLRF.Length
					+ chunkSize
					+ CLRF.Length;
		}

		/// <summary>
		/// Returns a chunk for upload consisting of the signed 'header' or chunk
		/// prefix plus the user data. The signature of the chunk incorporates the
		/// signature of the previous chunk (or, if the first chunk, the signature
		/// of the headers portion of the request).
		/// </summary>
		/// <param name="userDataLen">
		/// The length of the user data contained in userData
		/// </param>
		/// <param name="userData">
		/// Contains the user data to be sent in the upload chunk
		/// </param>
		/// <returns>
		/// A new buffer of data for upload containing the chunk header plus user data
		/// </returns>
		public byte[] ConstructSignedChunk(long userDataLen, byte[] userData)
		{
			// to keep our computation routine signatures simple, if the userData
			// buffer contains less data than it could, shrink it. Note the special case
			// to handle the requirement that we send an empty chunk to complete
			// our chunked upload.
			byte[] dataToChunk;
			if (userDataLen == 0)
				dataToChunk = FINAL_CHUNK;
			else
			{
				if (userDataLen < userData.Length)
				{
					// shrink the chunkdata to fit
					dataToChunk = new byte[userDataLen];
					Array.Copy(userData, 0, dataToChunk, 0, userDataLen);
				}
				else
					dataToChunk = userData;
			}

			var chunkHeader = new StringBuilder();

			// start with size of user data
			chunkHeader.Append(dataToChunk.Length.ToString("X"));

			// nonsig-extension; we have none in these samples
			const string nonsigExtension = "";

			// if this is the first chunk, we package it with the signing result
			// of the request headers, otherwise we use the cached signature
			// of the previous chunk

			// sig-extension
			var chunkStringToSign =
					CHUNK_STRING_TO_SIGN_PREFIX + "\n" +
					DateTimeStamp + "\n" +
					Scope + "\n" +
					LastComputedSignature + "\n" +
					ToHexString(CanonicalRequestHashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(nonsigExtension)), true) + "\n" +
					ToHexString(CanonicalRequestHashAlgorithm.ComputeHash(dataToChunk), true);

			// Console.WriteLine("\nChunkStringToSign:\n{0}", chunkStringToSign);

			// compute the V4 signature for the chunk
			var chunkSignature
				= ToHexString(ComputeKeyedHash("HMACSHA256",
											   SigningKey,
											   Encoding.UTF8.GetBytes(chunkStringToSign)),
									 true);

			// Console.WriteLine("\nChunkSignature:\n{0}", chunkSignature);

			// cache the signature to include with the next chunk's signature computation
			this.LastComputedSignature = chunkSignature;

			// construct the actual chunk, comprised of the non-signed extensions, the
			// 'headers' we just signed and their signature, plus a newline then copy
			// that plus the user's data to a payload to be written to the request stream
			chunkHeader.Append(nonsigExtension + CHUNK_SIGNATURE_HEADER + chunkSignature);
			chunkHeader.Append(CLRF);

			// Console.WriteLine("\nChunkHeader:\n{0}", chunkHeader);

			try
			{
				var header = Encoding.UTF8.GetBytes(chunkHeader.ToString());
				var trailer = Encoding.UTF8.GetBytes(CLRF);
				var signedChunk = new byte[header.Length + dataToChunk.Length + trailer.Length];

				Array.Copy(header, 0, signedChunk, 0, header.Length);
				Array.Copy(dataToChunk, 0, signedChunk, header.Length, dataToChunk.Length);
				Array.Copy(trailer, 0, signedChunk, header.Length + dataToChunk.Length, trailer.Length);

				// this is the total data for the chunk that will be sent to the request stream
				return signedChunk;
			}
			catch (Exception e)
			{
				throw new Exception("Unable to sign the chunked data. " + e.Message, e);
			}
		}
	}
}
