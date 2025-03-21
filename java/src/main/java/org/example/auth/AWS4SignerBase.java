package org.example.auth;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.example.Utility.NetUtils;

import com.amazonaws.util.BinaryUtils;

public abstract class AWS4SignerBase {

	public static final String EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
	public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

	public static final String SCHEME = "AWS4";
	public static final String ALGORITHM = "HMAC-SHA256";
	public static final String TERMINATOR = "aws4Request";
	public static final String X_AMZ_CONTENT_SHA256 = "X-Amz-Content-SHA256";

	public static final String ISO8601_BASIC_FORMAT = "yyyyMMdd'T'HHmmss'Z'";
	public static final String DATE_STRING_FORMAT = "yyyyMMdd";

	protected URL endpointUrl;
	protected String httpMethod;
	protected String serviceName;
	protected String regionName;

	protected final SimpleDateFormat dateTimeFormat;
	protected final SimpleDateFormat dateStampFormat;

	public AWS4SignerBase(URL endpointUrl, String httpMethod, String serviceName, String regionName) {
		this.endpointUrl = endpointUrl;
		this.httpMethod = httpMethod;
		this.serviceName = serviceName;
		this.regionName = regionName;

		dateTimeFormat = new SimpleDateFormat(ISO8601_BASIC_FORMAT);
		dateTimeFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
		dateStampFormat = new SimpleDateFormat(DATE_STRING_FORMAT);
		dateStampFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
	}

	protected static String getCanonicalizeHeaderNames(Map<String, String> headers) {
		List<String> sortedHeaders = new ArrayList<>();
		sortedHeaders.addAll(headers.keySet());
		Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

		StringBuilder buffer = new StringBuilder();
		for (String header : sortedHeaders) {
			if (buffer.length() > 0)
				buffer.append(";");
			buffer.append(header.toLowerCase());
		}

		return buffer.toString();
	}

	/**
	 * Computes the canonical headers with values for the request. For AWS4, all
	 * headers must be included in the signing process.
	 */
	protected static String getCanonicalizedHeaderString(Map<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			return "";
		}

		// step1: sort the headers by case-insensitive order
		List<String> sortedHeaders = new ArrayList<>();
		sortedHeaders.addAll(headers.keySet());
		Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

		// step2: form the canonical header:value entries in sorted order.
		// Multiple white spaces in the values should be compressed to a single
		// space.
		StringBuilder buffer = new StringBuilder();
		for (String key : sortedHeaders) {
			buffer.append(key.toLowerCase().replaceAll("\\s+", " ") + ":" + headers.get(key).replaceAll("\\s+", " "));
			buffer.append("\n");
		}

		return buffer.toString();
	}

	/**
	 * Returns the canonical request string to go into the signer process; this
	 * consists of several canonical sub-parts.
	 * 
	 * @return
	 */
	protected static String getCanonicalRequest(URL endpoint, String httpMethod, String queryParameters,
			String canonicalizedHeaderNames, String canonicalizedHeaders, String bodyHash) {
		return httpMethod + "\n" +
				getCanonicalizedResourcePath(endpoint) + "\n" +
				queryParameters + "\n" +
				canonicalizedHeaders + "\n" +
				canonicalizedHeaderNames + "\n" +
				bodyHash;
	}

	/**
	 * Returns the canonicalized resource path for the service endpoint.
	 */
	protected static String getCanonicalizedResourcePath(URL endpoint) {
		if (endpoint == null) {
			return "/";
		}
		String path = endpoint.getPath();
		if (path == null || path.isEmpty()) {
			return "/";
		}

		String encodedPath = NetUtils.urlEncode(path, true);
		if (encodedPath.startsWith("/")) {
			return encodedPath;
		} else {
			return "/".concat(encodedPath);
		}
	}

	/**
	 * Examines the specified query string parameters and returns a
	 * canonicalized form.
	 * <p>
	 * The canonicalized query string is formed by first sorting all the query
	 * string parameters, then URI encoding both the key and value and then
	 * joining them, in order, separating key value pairs with an '&'.
	 *
	 * @param parameters
	 *                   The query string parameters to be canonicalized.
	 *
	 * @return A canonicalized form for the specified query string parameters.
	 */
	public static String getCanonicalizedQueryString(Map<String, String> parameters) {
		if (parameters == null || parameters.isEmpty()) {
			return "";
		}

		var sorted = new TreeMap<String, String>();

		var pairs = parameters.entrySet().iterator();
		while (pairs.hasNext()) {
			var pair = pairs.next();
			var key = pair.getKey();
			var value = pair.getValue();
			sorted.put(NetUtils.urlEncode(key, false), NetUtils.urlEncode(value, false));
		}

		StringBuilder builder = new StringBuilder();
		pairs = sorted.entrySet().iterator();
		while (pairs.hasNext()) {
			var pair = pairs.next();
			builder.append(pair.getKey());
			builder.append("=");
			builder.append(pair.getValue());
			if (pairs.hasNext()) {
				builder.append("&");
			}
		}

		return builder.toString();
	}

	protected static String getStringToSign(String scheme, String algorithm, String dateTime, String scope,
			String canonicalRequest) {
		return scheme + "-" + algorithm + "\n" +
				dateTime + "\n" +
				scope + "\n" +
				BinaryUtils.toHex(hash(canonicalRequest));
	}

	public static byte[] hash(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(text.getBytes(StandardCharsets.UTF_8));
			return md.digest();
		} catch (Exception e) {
			throw new RuntimeException("Unable to compute hash while signing request: " + e.getMessage(), e);
		}
	}

	public static byte[] hash(byte[] data) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(data);
			return md.digest();
		} catch (Exception e) {
			throw new RuntimeException("Unable to compute hash while signing request: " + e.getMessage(), e);
		}
	}

	protected static byte[] sign(String stringData, byte[] key, String algorithm) {
		try {
			byte[] data = stringData.getBytes(StandardCharsets.UTF_8);
			Mac mac = Mac.getInstance(algorithm);
			mac.init(new SecretKeySpec(key, algorithm));
			return mac.doFinal(data);
		} catch (Exception e) {
			throw new RuntimeException("Unable to calculate a request signature: " + e.getMessage(), e);
		}
	}
}
