package org.example.replication;

import com.google.gson.JsonObject;

/** 포털에 등록된 리전. 복제기는 이 주소·키로 대상 버킷에 쓴다. */
public record PortalRegion(String name, String address, int port, int sslPort, String accessKey, String secretKey) {

	static PortalRegion fromApi(JsonObject d) {
		// SSL 포트 필드 이름: KSAN SSLPort, InfiniStor HttpsPort
		int ssl = Json.toInt(Json.getCi(d, "SSLPort"));
		if (ssl == 0)
			ssl = Json.toInt(Json.getCi(d, "HttpsPort"));
		return new PortalRegion(Json.toStr(Json.getCi(d, "Name")), Json.toStr(Json.getCi(d, "Address")),
				Json.toInt(Json.getCi(d, "Port")), ssl,
				Json.toStr(Json.getCi(d, "AccessKey")), Json.toStr(Json.getCi(d, "SecretKey")));
	}

	public String endpoint(boolean https) {
		return https ? "https://" + address + ":" + sslPort : "http://" + address + ":" + port;
	}
}
