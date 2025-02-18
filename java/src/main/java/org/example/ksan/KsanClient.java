/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.ksan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.example.auth.AWS4SignerBase;
import org.example.auth.AWS4SignerForAuthorizationHeader;

public class KsanClient {
	static final String METHOD_DELETE = "DELETE";
	static final String METHOD_POST = "POST";
	static final String METHOD_GET = "GET";
	static final String METHOD_LIST = "GET";
	static final String METHOD_PUT = "PUT";

	static final String HEADER_DATA = "NONE";
	static final String HEADER_BACKEND = "x-ifs-admin";
	static final String HEADER_AUTHORIZATION = "Authorization";
	static final String HEADER_CONTENT_LENGTH = "content-length";
	static final String HEADER_CONTENT_TYPE = "content-type";
	static final String DEFAULT_CONTENT_TYPE = "text/plain";

	final String host;
	final int port;
	final String accessKey;
	final String secretKey;

	public KsanClient(String host, int port, String accessKey, String secretKey) {
		this.host = host;
		this.port = port;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	Map<String, String> createHeaders(String query) {

		var headers = new HashMap<String, String>();
		headers.put(query, "");
		return headers;
	}

	/**
	 * 버킷의 태그 인덱스 삭제
	 * 
	 * @param bucketName
	 * @throws IOException
	 */
	public void deleteBucketTagIndex(String bucketName) throws IOException {
		var query = createHeaders("tag-index");
		var uri = new URL(String.format("http://%s:%d/%s/?%s", host, port, bucketName, query));

		var headers = new HashMap<String, String>();
		headers.put(AWS4SignerBase.X_AMZ_CONTENT_SHA256, AWS4SignerBase.EMPTY_BODY_SHA256);
		headers.put(HEADER_CONTENT_LENGTH, "0");
		headers.put(HEADER_CONTENT_TYPE, DEFAULT_CONTENT_TYPE);

		var signer = new AWS4SignerForAuthorizationHeader(uri, METHOD_DELETE, "s3", "us-west-2");

		var authorization = signer.computeSignature(headers, query, AWS4SignerBase.EMPTY_BODY_SHA256, accessKey, secretKey);
		headers.put(HEADER_AUTHORIZATION, authorization);

		delete(uri, headers);
	}

	// #region API
	public void post(URL endpoint, Map<String, String> headers, String body) throws IOException {
		sendRequest(METHOD_POST, endpoint, headers, body);
	}

	public void put(URL endpoint, Map<String, String> headers, String body) throws IOException {
		sendRequest(METHOD_PUT, endpoint, headers, body);
	}

	public <T> T get(URL endpoint, Map<String, String> headers, Class<T> responseType)
			throws IOException, JAXBException {
		return sendRequest(METHOD_GET, endpoint, headers, null, responseType);
	}

	public <T> T list(URL endpoint, Map<String, String> headers, Class<T> responseType)
			throws IOException, JAXBException {
		return sendRequest(METHOD_LIST, endpoint, headers, null, responseType);
	}

	public void delete(URL endpoint, Map<String, String> headers) throws IOException {
		sendRequest(METHOD_DELETE, endpoint, headers, null);
	}

	private void sendRequest(String method, URL endpoint, Map<String, String> headers, String body) throws IOException {
		var connection = (HttpURLConnection) endpoint.openConnection();
		connection.setRequestMethod(method);
		for (var header : headers.entrySet()) {
			connection.setRequestProperty(header.getKey(), header.getValue());
		}
		if (body != null) {
			connection.setDoOutput(true);
			try (OutputStream os = connection.getOutputStream()) {
				byte[] input = body.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}
		}
		int responseCode = connection.getResponseCode();
		if (responseCode != HttpURLConnection.HTTP_OK && responseCode != HttpURLConnection.HTTP_NO_CONTENT) {
			throw new RuntimeException("Request failed with response code: " + responseCode);
		}
	}

	private <T> T sendRequest(String method, URL endpoint, Map<String, String> headers, String body,
			Class<T> responseType) throws IOException, JAXBException {
		var connection = (HttpURLConnection) endpoint.openConnection();
		connection.setRequestMethod(method);
		for (var header : headers.entrySet()) {
			connection.setRequestProperty(header.getKey(), header.getValue());
		}
		if (body != null) {
			connection.setDoOutput(true);
			try (OutputStream os = connection.getOutputStream()) {
				byte[] input = body.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}
		}
		int responseCode = connection.getResponseCode();
		if (responseCode != HttpURLConnection.HTTP_OK) {
			throw new RuntimeException("Request failed with response code: " + responseCode);
		}
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
			var response = new StringBuilder();
			String responseLine;
			while ((responseLine = br.readLine()) != null) {
				response.append(responseLine.trim());
			}
			var jaxbContext = JAXBContext.newInstance(responseType);
			var unmarshaller = jaxbContext.createUnmarshaller();
			return responseType.cast(unmarshaller.unmarshal(new java.io.StringReader(response.toString())));
		}
	}
	// #endregion

}
