package org.example.replication;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * 포털 호출. KSAN(/api/v2, Bearer)과 InfiniStor(/api/v1, 키 그대로)를 [portal] type 으로 나눈다.
 * 응답은 둘 다 {"Data": ...} 봉투이고 리전·버킷 필드 이름(Name·Address·Port·FileCount·UsedSize)도 같다.
 */
public final class PortalClient {
	// KSAN 포털
	static final String PORTAL_BUCKET_PATH = "/api/v2/S3Api/Buckets/%s";
	static final String PORTAL_DEFAULT_REGION_PATH = "/api/v2/Regions/Default"; // 원본 시스템(내 리전)
	static final String PORTAL_REGIONS_PATH = "/api/v2/Regions";
	static final String PORTAL_AUTH_SCHEME = "Bearer";
	// InfiniStor 포털. 내 리전을 알려 주는 API 가 없어 [portal] region 또는 [s3] url 주소로 고른다
	static final String IFS_BUCKET_PATH = "/api/v1/ExternalApi/Buckets/%s"; // 버킷 주인 키(accessKey·secretKey 헤더)로 조회
	static final String IFS_REGIONS_PATH = "/api/v1/Region";
	static final long IFS_RESULT_SUCCESS = 1;

	static {
		// 테스트 서버는 자체 서명 인증서에 IP 로 접속하므로 호스트 이름 검증도 끈다(HttpClient 생성 전에 정해야 한다)
		System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");
	}

	private final Settings settings;
	private final boolean isIfs;
	private final String auth;
	private final HttpClient http;

	public PortalClient(Settings settings) {
		this.settings = settings;
		this.isIfs = settings.portalType.equals("ifs");
		this.auth = isIfs ? settings.apiKey : PORTAL_AUTH_SCHEME + " " + settings.apiKey;
		this.http = HttpClient.newBuilder().sslContext(trustAll()).connectTimeout(Common.REQUEST_TIMEOUT).build();
	}

	private static SSLContext trustAll() {
		try {
			var ctx = SSLContext.getInstance("TLS");
			ctx.init(null, new TrustManager[] { new X509TrustManager() {
				public void checkClientTrusted(X509Certificate[] chain, String authType) {
				}

				public void checkServerTrusted(X509Certificate[] chain, String authType) {
				}

				public X509Certificate[] getAcceptedIssuers() {
					return new X509Certificate[0];
				}
			} }, new SecureRandom());
			return ctx;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private JsonElement get(String path, Map<String, String> headers) {
		String url = settings.portalUrl + path;
		var req = HttpRequest.newBuilder(URI.create(url)).timeout(Common.REQUEST_TIMEOUT).header("Authorization", auth).GET();
		headers.forEach(req::header);
		HttpResponse<String> r;
		try {
			r = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
		} catch (IOException e) {
			throw new CaseFailed("포털 요청 실패: GET " + url + " (" + e + ")", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new CaseFailed("포털 요청 중단: GET " + url, e);
		}
		if (r.statusCode() != 200)
			throw new CaseFailed("포털 요청 실패: GET " + url + " → " + r.statusCode() + " " + Json.shorten(r.body(), 300));
		JsonElement body;
		try {
			body = JsonParser.parseString(r.body());
		} catch (JsonParseException e) {
			throw new CaseFailed("포털 응답이 JSON 이 아님: GET " + url + " → " + Json.shorten(r.body(), 300), e);
		}
		if (body.isJsonObject()) {
			JsonObject o = body.getAsJsonObject();
			// InfiniStor 는 실패해도 HTTP 200 에 Result 가 1 이 아니다
			JsonElement result = Json.getCi(o, "Result");
			if (isIfs && result != null && Json.toLong(result) != IFS_RESULT_SUCCESS)
				throw new CaseFailed("포털 요청 실패: GET " + url + " → Result " + Json.toStr(result) + " "
						+ Json.toStr(Json.getCi(o, "Code")) + " " + Json.toStr(Json.getCi(o, "Message")));
			// ResponseData 봉투({"Data": ...})로 오거나 내용이 바로 온다
			JsonElement data = Json.getCi(o, "Data");
			if (data != null)
				return data;
		}
		return body;
	}

	/**
	 * 원본 리전(내 리전). [portal] region 이 있으면 그 이름, 없으면 KSAN 은 기본 리전 API,
	 * InfiniStor 는 [s3] url 의 주소·포트와 같은 리전이 하나뿐일 때 그 리전.
	 */
	public PortalRegion defaultRegion() {
		if (!settings.homeRegion.isEmpty()) {
			List<PortalRegion> all = regions();
			return all.stream().filter(r -> r.name().equals(settings.homeRegion)).findFirst()
					.orElseThrow(() -> new ConfigError("[portal] region '" + settings.homeRegion
							+ "' 이 포털 리전 목록에 없음 (" + names(all) + ")"));
		}
		if (isIfs)
			return ifsHomeRegion();
		JsonElement data = get(PORTAL_DEFAULT_REGION_PATH, Map.of());
		if (!data.isJsonObject() || Json.toStr(Json.getCi(data.getAsJsonObject(), "Address")).isEmpty())
			throw new CaseFailed("포털 기본 리전 응답에 Address 가 없음: " + data);
		return PortalRegion.fromApi(data.getAsJsonObject());
	}

	private PortalRegion ifsHomeRegion() {
		List<PortalRegion> regions = regions();
		if (settings.s3Url.isEmpty())
			throw new ConfigError("InfiniStor 포털은 내 리전 API 가 없어 [portal] region 또는 [s3] url 이 필요함");
		URI uri = URI.create(settings.s3Url);
		String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase();
		int port = uri.getPort();
		List<PortalRegion> matched = regions.stream()
				.filter(r -> r.address().equalsIgnoreCase(host) && port >= 0 && (port == r.port() || port == r.sslPort()))
				.toList();
		if (matched.size() != 1)
			throw new ConfigError("[s3] url 주소와 같은 리전이 " + matched.size() + "개라 원본 리전을 정할 수 없음 "
					+ "([portal] region 에 이름을 적는다: " + names(matched.isEmpty() ? regions : matched) + ")");
		return matched.get(0);
	}

	private static String names(List<PortalRegion> regions) {
		String s = regions.stream().map(PortalRegion::name).collect(Collectors.joining(", "));
		return s.isEmpty() ? "없음" : s;
	}

	/** 리전 목록. 같은 이름이 주소(IP·도메인)만 달리해 여러 번 오면 하나로 줄이고 IP 주소를 고른다. */
	public List<PortalRegion> regions() {
		JsonElement data = get(isIfs ? IFS_REGIONS_PATH : PORTAL_REGIONS_PATH, Map.of());
		if (data.isJsonObject()) {
			JsonElement items = Json.getCi(data.getAsJsonObject(), "Items");
			data = items == null ? new com.google.gson.JsonArray() : items;
		}
		Map<String, PortalRegion> byName = new LinkedHashMap<>();
		if (data.isJsonArray()) {
			for (JsonElement e : data.getAsJsonArray()) {
				if (!e.isJsonObject())
					continue;
				PortalRegion region = PortalRegion.fromApi(e.getAsJsonObject());
				PortalRegion current = byName.get(region.name());
				if (current == null || (!isIp(current.address()) && isIp(region.address())))
					byName.put(region.name(), region);
			}
		}
		return new ArrayList<>(byName.values());
	}

	/** 버킷 usage. 포털은 AccessKey·SecretKey 헤더의 사용자로 버킷을 찾으므로 버킷 주인의 키를 준다. */
	public Usage bucketUsage(String bucket, String accessKey, String secretKey) {
		String path = String.format(isIfs ? IFS_BUCKET_PATH : PORTAL_BUCKET_PATH, bucket);
		JsonElement data = get(path, Map.of(
				"AccessKey", accessKey == null ? settings.accessKey : accessKey,
				"SecretKey", secretKey == null ? settings.secretKey : secretKey));
		if (!data.isJsonObject() || Json.getCi(data.getAsJsonObject(), "FileCount") == null)
			throw new CaseFailed("포털 응답에 FileCount 가 없음: " + Json.shorten(data, 300));
		JsonObject o = data.getAsJsonObject();
		return new Usage(Json.toLong(Json.getCi(o, "FileCount")), Json.toLong(Json.getCi(o, "UsedSize")));
	}

	public Usage bucketUsage(String bucket) {
		return bucketUsage(bucket, null, null);
	}

	/** IP 주소 표기인지(IPv4 점 표기 또는 IPv6). 이름 조회는 하지 않는다. */
	static boolean isIp(String address) {
		if (address.matches("\\d{1,3}(\\.\\d{1,3}){3}")) {
			for (String part : address.split("\\."))
				if (Integer.parseInt(part) > 255)
					return false;
			return true;
		}
		return address.contains(":") && address.matches("[0-9a-fA-F:.]+");
	}
}
