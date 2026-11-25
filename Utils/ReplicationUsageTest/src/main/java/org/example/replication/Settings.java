package org.example.replication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** config.ini 설정. */
public final class Settings {
	public static final List<String> PORTAL_TYPES = List.of("ksan", "ifs");
	public static final String DEFAULT_PORTAL_TYPE = "ksan";
	private static final Map<String, String> PORTAL_TYPE_ALIASES = Map.of("infinistor", "ifs");
	private static final Pattern S3_URL_RE = Pattern.compile("https?://[^/\\s]+");

	public final String portalUrl;
	public final String apiKey;
	public final String portalType;
	public final String homeRegion;
	public final String accessKey;
	public final String secretKey;
	public final boolean useHttps;
	public final String s3Url;
	public final int fileCount;
	public final List<Long> fileSizes;

	private Settings(Ini ini) {
		String countText = ini.get("test", "file_count", false);
		int count;
		try {
			count = countText.isEmpty() ? Common.DEFAULT_FILE_COUNT : Integer.parseInt(countText);
		} catch (NumberFormatException e) {
			throw new ConfigError("[test] file_count 가 정수가 아님: '" + countText + "'");
		}
		if (count < 1)
			throw new ConfigError("[test] file_count 는 1 이상이어야 함");
		String sizesText = ini.get("test", "file_sizes", false);
		if (sizesText.isEmpty())
			sizesText = Common.DEFAULT_FILE_SIZES;
		List<Long> sizes = new ArrayList<>();
		for (String s : sizesText.split(","))
			if (!s.isBlank())
				sizes.add(Common.parseSize(s));
		if (sizes.isEmpty())
			throw new ConfigError("[test] file_sizes 에 크기가 없음");
		String httpsText = ini.get("s3", "https", false).toLowerCase();
		if (!List.of("", "true", "false").contains(httpsText))
			throw new ConfigError("[s3] https 는 true/false: '" + httpsText + "'");
		String url = ini.get("s3", "url", false).replaceAll("/+$", "");
		if (!url.isEmpty() && !S3_URL_RE.matcher(url).matches())
			throw new ConfigError("[s3] url 은 http(s)://호스트:포트 형식: '" + url + "'");

		String type = ini.get("portal", "type", false).toLowerCase();
		if (type.isEmpty())
			type = DEFAULT_PORTAL_TYPE;
		type = PORTAL_TYPE_ALIASES.getOrDefault(type, type);
		if (!PORTAL_TYPES.contains(type))
			throw new ConfigError("[portal] type 은 " + String.join("·", PORTAL_TYPES) + " 중 하나: '" + type + "'");

		portalUrl = ini.get("portal", "url", true).replaceAll("/+$", "");
		apiKey = ini.get("portal", "api_key", true);
		portalType = type;
		homeRegion = ini.get("portal", "region", false);
		accessKey = ini.get("s3", "access_key", true);
		secretKey = ini.get("s3", "secret_key", true);
		useHttps = httpsText.equals("true");
		s3Url = url;
		fileCount = count;
		fileSizes = List.copyOf(sizes);
	}

	public static Settings load(Path path) {
		if (!Files.isRegularFile(path))
			throw new ConfigError("설정 파일이 없음: " + path + " (config.ini.example 을 복사해 채운다)");
		try {
			return new Settings(Ini.parse(Files.readString(path, StandardCharsets.UTF_8)));
		} catch (IOException e) {
			throw new ConfigError("설정 파일을 읽지 못함: " + path + " (" + e.getMessage() + ")");
		}
	}

	/** file_sizes 를 file_count 만큼 돌아가며 배정한 크기 목록. */
	public List<Long> objectSizes() {
		List<Long> sizes = new ArrayList<>();
		for (int i = 0; i < fileCount; i++)
			sizes.add(fileSizes.get(i % fileSizes.size()));
		return sizes;
	}

	/** 원본 S3 주소와 출처. [s3] url 이 있으면 그 값, 없으면 포털 기본 리전 주소. */
	public String[] sourceEndpoint(PortalRegion home) {
		if (!s3Url.isEmpty())
			return new String[] { s3Url, "config" };
		return new String[] { home.endpoint(useHttps), "포털 기본 리전" };
	}

	/** 섹션·키(대소문자 무시)만 읽는 간단한 ini. 주석은 줄 맨 앞의 # 또는 ; */
	static final class Ini {
		private final Map<String, Map<String, String>> sections = new HashMap<>();

		static Ini parse(String text) {
			var ini = new Ini();
			Map<String, String> current = null;
			for (String raw : text.replace("﻿", "").split("\\R")) {
				String line = raw.strip();
				if (line.isEmpty() || line.startsWith("#") || line.startsWith(";"))
					continue;
				if (line.startsWith("[") && line.endsWith("]")) {
					current = ini.sections.computeIfAbsent(line.substring(1, line.length() - 1).strip(),
							k -> new HashMap<>());
					continue;
				}
				int eq = line.indexOf('=');
				int colon = line.indexOf(':');
				int sep = eq < 0 ? colon : (colon < 0 ? eq : Math.min(eq, colon));
				if (current == null || sep < 0)
					continue;
				current.put(line.substring(0, sep).strip().toLowerCase(), line.substring(sep + 1).strip());
			}
			return ini;
		}

		String get(String section, String key, boolean required) {
			String value = sections.getOrDefault(section, Map.of()).getOrDefault(key, "").strip();
			if (required && value.isEmpty())
				throw new ConfigError("[" + section + "] " + key + " 값이 비어 있음");
			return value;
		}
	}
}
