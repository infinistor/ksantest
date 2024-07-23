package org.example.Utility;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.example.Data.FormFile;
import org.example.Data.MainData;
import org.example.Data.MyResult;
import org.example.auth.AWS4SignerForChunkedUpload;

/**
 * Various Http helper routines
 */
public class NetUtils {

	NetUtils() {
	}

	public static final int USER_DATE_BLOCK_SIZE = 64 * 1024;
	public static final String LINE_FEED = "\r\n";

	public static String createRegion2Http(String region) {
		return String.format("http://s3.%s.amazonaws.com", region);
	}

	public static String createRegion2Https(String regionName) {
		return String.format("https://s3.%s.amazonaws.com", regionName);
	}

	public static String createURLToHTTP(String address, int port) {
		var url = MainData.HTTP + address;

		if (url.endsWith("/"))
			url = url.substring(0, url.length() - 1);

		return String.format("%s:%d", url, port);
	}

	public static String createURLToHTTPS(String address, int port) {
		var url = MainData.HTTPS + address;

		if (url.endsWith("/"))
			url = url.substring(0, url.length() - 1);

		return String.format("%s:%d", url, port);
	}

	public static URL getEndpoint(String protocol, String address, int port, String bucketName)
			throws MalformedURLException {
		return new URL(String.format("%s%s:%d/%s", protocol, address, port, bucketName));
	}

	public static URL getEndpoint(String protocol, String regionName, String bucketName) throws MalformedURLException {
		return new URL(String.format("%s%s.s3-%s.amazonaws.com", protocol, bucketName, regionName));
	}

	public static URL getEndpoint(String protocol, String address, int port, String bucketName, String key)
			throws MalformedURLException {
		return new URL(String.format("%s%s:%d/%s/%s", protocol, address, port, bucketName, key));
	}

	public static URL getEndpoint(String protocol, String regionName, String bucketName, String key)
			throws MalformedURLException {
		return new URL(String.format("%s%s.s3-%s.amazonaws.com/%s", protocol, bucketName, regionName, key));
	}

	public static MyResult postUpload(URL sendURL, Map<String, String> headers, FormFile fileData) {
		var result = new MyResult();

		try {
			if (sendURL.getProtocol().startsWith("https")) {
				ignoreSsl();
			}

			var boundary = Long.toHexString(System.currentTimeMillis());
			var connection = (HttpURLConnection) sendURL.openConnection();

			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
			connection.setRequestMethod("POST");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			var writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8));

			for (var header : headers.entrySet()) {
				writer.append("--" + boundary).append(LINE_FEED);
				writer.append(String.format("Content-Disposition: form-data; name=\"%s\"", header.getKey()))
						.append(LINE_FEED);
				writer.append(LINE_FEED);
				writer.append(header.getValue()).append(LINE_FEED);
			}

			writer.append("--" + boundary).append(LINE_FEED);
			writer.append(
					String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"", fileData.name))
					.append(LINE_FEED);
			writer.append(String.format("Content-Type: %s", fileData.contentType)).append(LINE_FEED);
			writer.append(LINE_FEED);
			writer.append(fileData.body).append(LINE_FEED);
			writer.append("--" + boundary + "--").append(LINE_FEED);
			writer.close();

			result.statusCode = connection.getResponseCode();
			result.URL = connection.getURL().toString();
			if (result.statusCode != HttpURLConnection.HTTP_NO_CONTENT
					&& result.statusCode != HttpURLConnection.HTTP_CREATED
					&& result.statusCode != HttpURLConnection.HTTP_OK) {
				var in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
				var inputLine = "";
				var response = new StringBuilder();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				result.message = response.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public static MyResult putUpload(URL endpoint, String httpMethod, Map<String, String> headers, String requestBody) {
		try {
			var connection = createHttpConnection(endpoint, httpMethod, headers);
			if (requestBody != null) {
				var wr = new DataOutputStream(connection.getOutputStream());
				wr.writeBytes(requestBody);
				wr.flush();
				wr.close();
			}

			return send(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static MyResult putUploadChunked(URL endpoint, String httpMethod, Map<String, String> headers,
			AWS4SignerForChunkedUpload signer, String requestBody) {

		try {
			var connection = NetUtils.createHttpConnection(endpoint, httpMethod, headers);

			var buffer = new byte[USER_DATE_BLOCK_SIZE];
			var outputStream = new DataOutputStream(connection.getOutputStream());

			var inputStream = new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8));
			int bytesRead = 0;
			while ((bytesRead = inputStream.read(buffer, 0, buffer.length)) != -1) {
				byte[] chunk = signer.constructSignedChunk(bytesRead, buffer);
				outputStream.write(chunk);
				outputStream.flush();
			}

			byte[] finalChunk = signer.constructSignedChunk(0, buffer);
			outputStream.write(finalChunk);
			outputStream.flush();
			outputStream.close();

			return send(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static MyResult send(HttpURLConnection connection) {
		var result = new MyResult();

		try (InputStream is = connection.getResponseCode() == HttpURLConnection.HTTP_OK ? connection.getInputStream()
				: connection.getErrorStream();) {

			var rd = new BufferedReader(new InputStreamReader(is));
			var line = "";
			var response = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				response.append(line);
				response.append('\r');
			}
			rd.close();
			result.message = response.toString();
			result.statusCode = connection.getResponseCode();
		} catch (Exception e) {
			e.printStackTrace();
			result.message = e.getMessage();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
		return result;
	}

	public static HttpURLConnection createHttpConnection(URL endpointUrl, String httpMethod,
			Map<String, String> headers) {
		try {
			HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
			connection.setRequestMethod(httpMethod);

			if (headers != null) {
				for (var header : headers.entrySet()) {
					connection.setRequestProperty(header.getKey(), header.getValue());
				}
			}

			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			return connection;
		} catch (Exception e) {
			throw new RuntimeException("Cannot create connection. " + e.getMessage(), e);
		}
	}

	public static String urlEncode(String url, boolean keepPathSlash) {
		String encoded;
		try {
			encoded = URLEncoder.encode(url, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 encoding is not supported.", e);
		}
		if (keepPathSlash) {
			encoded = encoded.replace("%2F", "/");
		}
		return encoded;
	}

	private static void trustAllHttpsCertificates() throws Exception {
		var trustAllCerts = new TrustManager[1];
		var tm = new miTM();
		trustAllCerts[0] = tm;
		var sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	}

	static class miTM implements X509TrustManager {
		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}

		public boolean isServerTrusted(X509Certificate[] certs) {
			return true;
		}

		public boolean isClientTrusted(X509Certificate[] certs) {
			return true;
		}

		public void checkServerTrusted(X509Certificate[] certs, String authType)
				throws CertificateException {
			return;
		}

		public void checkClientTrusted(X509Certificate[] certs, String authType)
				throws CertificateException {
			return;
		}
	}

	public static void ignoreSsl() throws Exception {
		HostnameVerifier hv = new HostnameVerifier() {
			public boolean verify(String urlHostName, SSLSession session) {
				return true;
			}
		};
		trustAllHttpsCertificates();
		HttpsURLConnection.setDefaultHostnameVerifier(hv);
	}

}
