package org.example.Utility;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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

	public final static int USER_DATE_BLOCK_SIZE = 64 * 1024;

	public static String CreateURLToHTTP(String Address, int Port) {
		var URL = MainData.HTTP + Address;

		if (URL.endsWith("/"))
			URL = URL.substring(0, URL.length() - 1);

		return String.format("%s:%d", URL, Port);
	}

	public static String CreateURLToHTTPS(String Address, int Port) {
		var URL = MainData.HTTPS + Address;

		if (URL.endsWith("/"))
			URL = URL.substring(0, URL.length() - 1);

		return String.format("%s:%d", URL, Port);
	}

	public static URL GetEndPoint(String Protocol, String Address, int Port, String BucketName)
			throws MalformedURLException {
		return new URL(String.format("%s%s:%d/%s", Protocol, Address, Port, BucketName));
	}

	public static URL GetEndPoint(String Protocol, String RegionName, String BucketName) throws MalformedURLException {
		return new URL(String.format("%s%s.s3-%s.amazonaws.com", Protocol, BucketName, RegionName));
	}

	public static URL GetEndPoint(String Protocol, String Address, int Port, String BucketName, String Key)
			throws MalformedURLException {
		return new URL(String.format("%s%s:%d/%s/%s", Protocol, Address, Port, BucketName, Key));
	}

	public static URL GetEndPoint(String Protocol, String RegionName, String BucketName, String Key)
			throws MalformedURLException {
		return new URL(String.format("%s%s.s3-%s.amazonaws.com/%s", Protocol, BucketName, RegionName, Key));
	}

	public static MyResult PostUpload(URL SendURL, Map<String, String> Headers, FormFile FileData) {
		var Result = new MyResult();

		try {
			if (SendURL.getProtocol().startsWith("https")) {
				ignoreSsl();
			}

			var boundary = Long.toHexString(System.currentTimeMillis());
			var LINE_FEED = "\r\n";
			var connection = (HttpURLConnection) SendURL.openConnection();

			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
			connection.setRequestMethod("POST");
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setConnectTimeout(15000);

			var writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));

			for (var Header : Headers.keySet()) {
				writer.append("--" + boundary).append(LINE_FEED);
				writer.append(String.format("Content-Disposition: form-data; name=\"%s\"", Header)).append(LINE_FEED);
				writer.append(LINE_FEED);
				writer.append(Headers.get(Header)).append(LINE_FEED);
			}

			writer.append("--" + boundary).append(LINE_FEED);
			writer.append(String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"", FileData.Name)).append(LINE_FEED);
			writer.append(String.format("Content-Type: %s", FileData.ContentType)).append(LINE_FEED);
			writer.append(LINE_FEED);
			writer.append(FileData.Body).append(LINE_FEED);
			writer.append("--" + boundary + "--").append(LINE_FEED);
			writer.close();

			Result.StatusCode = connection.getResponseCode();
			Result.URL = connection.getURL().toString();
			if (Result.StatusCode != HttpURLConnection.HTTP_NO_CONTENT
					&& Result.StatusCode != HttpURLConnection.HTTP_CREATED
					&& Result.StatusCode != HttpURLConnection.HTTP_OK) {
				var in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
				var inputLine = "";
				var response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				Result.Message = response.toString();
			}
		} catch (IOException e) {
			fail(e.getMessage());
		} catch (Exception e) {
			fail(e.getMessage());
		}

		return Result;
	}

	public static MyResult PutUpload(URL EndPoint, String httpMethod, Map<String, String> headers, String requestBody) {
		try {
			var connection = createHttpConnection(EndPoint, httpMethod, headers);
			if (requestBody != null) {
				var wr = new DataOutputStream(connection.getOutputStream());
				wr.writeBytes(requestBody);
				wr.flush();
				wr.close();
			}

			return Send(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static MyResult PutUploadChunked(URL EndPoint, String httpMethod, Map<String, String> headers, AWS4SignerForChunkedUpload signer, String requestBody) {

		try {
			var connection = NetUtils.createHttpConnection(EndPoint, httpMethod, headers);

			var buffer = new byte[USER_DATE_BLOCK_SIZE];
			var outputStream = new DataOutputStream(connection.getOutputStream());

			var inputStream = new ByteArrayInputStream(requestBody.getBytes("UTF-8"));
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

			return Send(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static MyResult Send(HttpURLConnection connection) {
		var Result = new MyResult();
		try {
			// Get Response
			InputStream is;
			try {
				is = connection.getInputStream();
			} catch (IOException e) {
				is = connection.getErrorStream();
			}

			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer response = new StringBuffer();
			while ((line = rd.readLine()) != null) {
				response.append(line);
				response.append('\r');
			}
			rd.close();
			Result.Message = response.toString();
			Result.StatusCode = connection.getResponseCode();
		} catch (Exception e) {
			e.printStackTrace();
			Result.Message = e.getMessage();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
		return Result;
	}

	public static HttpURLConnection createHttpConnection(URL endpointUrl, String httpMethod,
			Map<String, String> headers) {
		try {
			HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
			connection.setRequestMethod(httpMethod);

			if (headers != null) {
				for (String headerKey : headers.keySet()) {
					connection.setRequestProperty(headerKey, headers.get(headerKey));
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
		TrustManager[] trustAllCerts = new TrustManager[1];
		TrustManager tm = new miTM();
		trustAllCerts[0] = tm;
		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, null);
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
