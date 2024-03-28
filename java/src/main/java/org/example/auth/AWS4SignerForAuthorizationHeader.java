package org.example.auth;

import java.net.URL;
import java.util.Date;
import java.util.Map;

import com.amazonaws.util.BinaryUtils;

public class AWS4SignerForAuthorizationHeader extends AWS4SignerBase {

	public AWS4SignerForAuthorizationHeader(URL endpointUrl, String httpMethod, String serviceName, String regionName) {
		super(endpointUrl, httpMethod, serviceName, regionName);
	}

	public String computeSignature(Map<String, String> headers, Map<String, String> queryParameters, String bodyHash, String accessKey, String secretKey) {
		Date now = new Date();
		String dateTimeStamp = dateTimeFormat.format(now);

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
		String canonicalRequest = getCanonicalRequest(endpointUrl, httpMethod, canonicalizedQueryParameters, canonicalizedHeaderNames, canonicalizedHeaders, bodyHash);

		String dateStamp = dateStampFormat.format(now);
		String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + TERMINATOR;
		String stringToSign = getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);

		// compute the signing key
		byte[] kSecret = (SCHEME + secretKey).getBytes();
		byte[] kDate = sign(dateStamp, kSecret, "HmacSHA256");
		byte[] kRegion = sign(regionName, kDate, "HmacSHA256");
		byte[] kService = sign(serviceName, kRegion, "HmacSHA256");
		byte[] kSigning = sign(TERMINATOR, kService, "HmacSHA256");
		byte[] signature = sign(stringToSign, kSigning, "HmacSHA256");

		String credentialsAuthorizationHeader = "Credential=" + accessKey + "/" + scope;
		String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
		String signatureAuthorizationHeader = "Signature=" + BinaryUtils.toHex(signature);

		return SCHEME + "-" + ALGORITHM + " " + credentialsAuthorizationHeader + ", " + signedHeadersAuthorizationHeader + ", " + signatureAuthorizationHeader;
	}
}
