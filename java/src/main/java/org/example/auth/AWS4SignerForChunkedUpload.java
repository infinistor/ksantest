package org.example.auth;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import com.amazonaws.util.BinaryUtils;

public class AWS4SignerForChunkedUpload extends AWS4SignerBase {
	public static final String STREAMING_BODY_SHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

	private static final String CLRF = "\r\n";
	private static final String CHUNK_STRING_TO_SIGN_PREFIX = "AWS4-HMAC-SHA256-PAYLOAD";
	private static final String CHUNK_SIGNATURE_HEADER = ";chunk-signature=";
	private static final int SIGNATURE_LENGTH = 64;
	private static final byte[] FINAL_CHUNK = new byte[0];

	private String lastComputedSignature;
	private String dateTimeStamp;
	private String scope;
	private byte[] signingKey;

	public AWS4SignerForChunkedUpload(URL endpointUrl, String httpMethod,
			String serviceName, String regionName) {
		super(endpointUrl, httpMethod, serviceName, regionName);
	}

	public String computeSignature(Map<String, String> headers, Map<String, String> queryParameters, String bodyHash, String accessKey, String secretKey) {
		Date now = new Date();
		this.dateTimeStamp = dateTimeFormat.format(now);

		headers.put("x-amz-date", dateTimeStamp);

		String hostHeader = endpointUrl.getHost();
		int port = endpointUrl.getPort();
		if (port > -1) {
			hostHeader = hostHeader.concat(":" + Integer.toString(port));
		}
		headers.put("Host", hostHeader);

		String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
		String canonicalizedHeaders = getCanonicalizedHeaderString(headers);
		String canonicalizedQueryParameters = getCanonicalizedQueryString(queryParameters);
		String canonicalRequest = getCanonicalRequest(endpointUrl, httpMethod,
				canonicalizedQueryParameters, canonicalizedHeaderNames,
				canonicalizedHeaders, bodyHash);

		String dateStamp = dateStampFormat.format(now);
		this.scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + TERMINATOR;
		String stringToSign = getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);

		byte[] kSecret = (SCHEME + secretKey).getBytes();
		byte[] kDate = sign(dateStamp, kSecret, "HmacSHA256");
		byte[] kRegion = sign(regionName, kDate, "HmacSHA256");
		byte[] kService = sign(serviceName, kRegion, "HmacSHA256");
		this.signingKey = sign(TERMINATOR, kService, "HmacSHA256");
		byte[] signature = sign(stringToSign, signingKey, "HmacSHA256");

		lastComputedSignature = BinaryUtils.toHex(signature);

		String credentialsAuthorizationHeader = "Credential=" + accessKey + "/" + scope;
		String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
		String signatureAuthorizationHeader = "Signature=" + lastComputedSignature;
		return SCHEME + "-" + ALGORITHM + " " + credentialsAuthorizationHeader + ", " + signedHeadersAuthorizationHeader + ", " + signatureAuthorizationHeader;
	}

	public static long calculateChunkedContentLength(long originalLength, long chunkSize) {
		if (originalLength <= 0) {
			throw new IllegalArgumentException("Nonnegative content length expected.");
		}

		long maxSizeChunks = originalLength / chunkSize;
		long remainingBytes = originalLength % chunkSize;
		return maxSizeChunks * calculateChunkHeaderLength(chunkSize)
				+ (remainingBytes > 0 ? calculateChunkHeaderLength(remainingBytes) : 0)
				+ calculateChunkHeaderLength(0);
	}

	private static long calculateChunkHeaderLength(long chunkDataSize) {
		return Long.toHexString(chunkDataSize).length()
				+ CHUNK_SIGNATURE_HEADER.length()
				+ SIGNATURE_LENGTH
				+ CLRF.length()
				+ chunkDataSize
				+ CLRF.length();
	}

	public byte[] constructSignedChunk(int userDataLen, byte[] userData) {
		byte[] dataToChunk;
		if (userDataLen == 0) {
			dataToChunk = FINAL_CHUNK;
		} else {
			if (userDataLen < userData.length) {
				// shrink the chunk data to fit
				dataToChunk = new byte[userDataLen];
				System.arraycopy(userData, 0, dataToChunk, 0, userDataLen);
			} else {
				dataToChunk = userData;
			}
		}

		StringBuilder chunkHeader = new StringBuilder();

		// start with size of user data
		chunkHeader.append(Integer.toHexString(dataToChunk.length));

		// nonSign-extension; we have none in these samples
		String nonSignExtension = "";

		// sig-extension
		String chunkStringToSign = CHUNK_STRING_TO_SIGN_PREFIX + "\n" +
				dateTimeStamp + "\n" +
				scope + "\n" +
				lastComputedSignature + "\n" +
				BinaryUtils.toHex(AWS4SignerBase.hash(nonSignExtension)) + "\n" +
				BinaryUtils.toHex(AWS4SignerBase.hash(dataToChunk));

		String chunkSignature = BinaryUtils.toHex(AWS4SignerBase.sign(chunkStringToSign, signingKey, "HmacSHA256"));
		lastComputedSignature = chunkSignature;

		chunkHeader.append(nonSignExtension + CHUNK_SIGNATURE_HEADER + chunkSignature);
		chunkHeader.append(CLRF);

		try {
			byte[] header = chunkHeader.toString().getBytes(StandardCharsets.UTF_8);
			byte[] trailer = CLRF.getBytes(StandardCharsets.UTF_8);
			byte[] signedChunk = new byte[header.length + dataToChunk.length + trailer.length];
			System.arraycopy(header, 0, signedChunk, 0, header.length);
			System.arraycopy(dataToChunk, 0, signedChunk, header.length, dataToChunk.length);
			System.arraycopy(trailer, 0, signedChunk, header.length + dataToChunk.length, trailer.length);

			return signedChunk;
		} catch (Exception e) {
			throw new RuntimeException("Unable to sign the chunked data. " + e.getMessage(), e);
		}
	}
}