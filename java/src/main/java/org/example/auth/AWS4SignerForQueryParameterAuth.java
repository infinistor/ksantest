package org.example.auth;

import java.net.URL;
import java.util.Date;
import java.util.Map;

import com.amazonaws.util.BinaryUtils;

public class AWS4SignerForQueryParameterAuth extends AWS4SignerBase {

	public AWS4SignerForQueryParameterAuth(URL endpointUrl, String httpMethod, String serviceName, String regionName) {
		super(endpointUrl, httpMethod, serviceName, regionName);
	}

	public String computeSignature(Map<String, String> headers, Map<String, String> parameters, String bodyHash, String accessKey, String secretKey) {
		Date now = new Date();
		String dateTimeStamp = dateTimeFormat.format(now);

		String hostHeader = endpointUrl.getHost();
		int port = endpointUrl.getPort();
		if (port > -1) {
			hostHeader = hostHeader.concat(":" + Integer.toString(port));
		}
		headers.put("Host", hostHeader);

		String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
		String canonicalizedHeaders = getCanonicalizedHeaderString(headers);

		String dateStamp = dateStampFormat.format(now);
		String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + TERMINATOR;

		parameters.put("X-Amz-Algorithm", SCHEME + "-" + ALGORITHM);
		parameters.put("X-Amz-Credential", accessKey + "/" + scope);
		parameters.put("X-Amz-Date", dateTimeStamp);
		parameters.put("X-Amz-SignedHeaders", canonicalizedHeaderNames);

		String canonicalizedQueryParameters = getCanonicalizedQueryString(parameters);
		String canonicalRequest = getCanonicalRequest(endpointUrl, httpMethod,
				canonicalizedQueryParameters, canonicalizedHeaderNames,
				canonicalizedHeaders, bodyHash);

		String stringToSign = getStringToSign(SCHEME, ALGORITHM, dateTimeStamp, scope, canonicalRequest);

		byte[] kSecret = (SCHEME + secretKey).getBytes();
		byte[] kDate = sign(dateStamp, kSecret, "HmacSHA256");
		byte[] kRegion = sign(regionName, kDate, "HmacSHA256");
		byte[] kService = sign(serviceName, kRegion, "HmacSHA256");
		byte[] kSigning = sign(TERMINATOR, kService, "HmacSHA256");
		byte[] signature = sign(stringToSign, kSigning, "HmacSHA256");

		StringBuilder authString = new StringBuilder();

		authString.append("X-Amz-Algorithm=" + parameters.get("X-Amz-Algorithm"));
		authString.append("&X-Amz-Credential=" + parameters.get("X-Amz-Credential"));
		authString.append("&X-Amz-Date=" + parameters.get("X-Amz-Date"));
		authString.append("&X-Amz-Expires=" + parameters.get("X-Amz-Expires"));
		authString.append("&X-Amz-SignedHeaders=" + parameters.get("X-Amz-SignedHeaders"));
		authString.append("&X-Amz-Signature=" + BinaryUtils.toHex(signature));

		return authString.toString();
	}
}
