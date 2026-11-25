package org.example.replication;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.utils.AttributeMap;

/** 복제 usage 테스트의 공용 부분: 기본값, 크기 표기, S3 클라이언트, 대기·정리 함수. */
public final class Common {
	// ── 기본값 ────────────────────────────────────────────────────────
	public static final int DEFAULT_FILE_COUNT = 10;
	public static final String DEFAULT_FILE_SIZES = "0,1K,64K,1M";
	public static final long MAX_FILE_SIZE = 5L * 1024 * 1024 * 1024; // GW MaxPutSize(5 GiB). 단일 PUT 상한

	public static final String BUCKET_PREFIX = "rtest";
	public static final Region S3_REGION = Region.US_EAST_1;
	public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
	// 테스트 서버는 자체 서명 인증서라 검증하지 않는다

	public static final int REPLICATION_TIMEOUT_SEC = 300;
	public static final int REPLICATION_POLL_SEC = 5;
	public static final int USAGE_TIMEOUT_SEC = 300;
	public static final int USAGE_POLL_SEC = 10;
	public static final boolean KEEP_BUCKETS_ON_FAIL = true;

	private static final Map<String, Long> SIZE_UNITS = Map.of("", 1L, "B", 1L, "K", 1024L, "M", 1024L * 1024,
			"G", 1024L * 1024 * 1024);
	private static final Pattern SIZE_RE = Pattern.compile("\\s*(\\d+)\\s*([BKMG]?)\\s*");
	private static final DateTimeFormatter BUCKET_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private static final Pattern BUCKET_TIME_RE = Pattern.compile("^" + BUCKET_PREFIX + "-.+-(\\d{14})$");
	private static final SecureRandom RANDOM = new SecureRandom();

	private Common() {
	}

	// ── 크기 ─────────────────────────────────────────────────────────
	public static long parseSize(String text) {
		Matcher m = SIZE_RE.matcher(text.toUpperCase());
		if (!m.matches())
			throw new ConfigError("file_sizes 의 크기 형식이 잘못됨: '" + text + "' (예: 0, 512B, 64K, 1M, 1G)");
		long size;
		try {
			size = Math.multiplyExact(Long.parseLong(m.group(1)), SIZE_UNITS.get(m.group(2)));
		} catch (ArithmeticException | NumberFormatException e) {
			size = Long.MAX_VALUE;
		}
		if (size > MAX_FILE_SIZE)
			throw new ConfigError("file_sizes 의 '" + text + "' 가 단일 PUT 상한 5G 를 넘음");
		return size;
	}

	public static String formatSize(long size) {
		for (String unit : new String[] { "G", "M", "K" }) {
			long u = SIZE_UNITS.get(unit);
			if (size >= u && size % u == 0)
				return (size / u) + unit;
		}
		return size + "B";
	}

	// ── 클라이언트 ────────────────────────────────────────────────────
	public static S3Client makeS3Client(String endpoint, String accessKey, String secretKey) {
		var http = ApacheHttpClient.builder()
				.connectionTimeout(REQUEST_TIMEOUT)
				.socketTimeout(REQUEST_TIMEOUT)
				.buildWithDefaults(AttributeMap.builder()
						.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());
		return S3Client.builder()
				.endpointOverride(URI.create(endpoint))
				.region(S3_REGION)
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
				.httpClient(http)
				// path 방식, aws-chunked 없이 보낸다. 체크섬은 케이스가 직접 줄 때만 붙는다
				.serviceConfiguration(S3Configuration.builder()
						.pathStyleAccessEnabled(true)
						.chunkedEncodingEnabled(false)
						.build())
				.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
				.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
				.overrideConfiguration(o -> o.retryStrategy(r -> r.maxAttempts(3)))
				.build();
	}

	// ── S3 보조 함수 ──────────────────────────────────────────────────
	public static String newBucketName(String kind) {
		return BUCKET_PREFIX + "-" + kind + "-" + LocalDateTime.now().format(BUCKET_TIME);
	}

	/** newBucketName 으로 만든 이름이면 이름의 시각, 아니면 null. */
	public static LocalDateTime bucketCreatedAt(String bucket) {
		Matcher m = BUCKET_TIME_RE.matcher(bucket);
		if (!m.matches())
			return null;
		try {
			return LocalDateTime.parse(m.group(1), BUCKET_TIME);
		} catch (DateTimeParseException e) {
			return null;
		}
	}

	public static byte[] randomBytes(int size) {
		byte[] body = new byte[size];
		RANDOM.nextBytes(body);
		return body;
	}

	public static String md5Hex(byte[] data) {
		return HexFormat.of().formatHex(digest("MD5", data));
	}

	public static byte[] digest(String algorithm, byte[] data) {
		try {
			return MessageDigest.getInstance(algorithm).digest(data);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	/** ETag 양끝 따옴표를 뗀다. */
	public static String stripQuotes(String etag) {
		return etag == null ? null : etag.replace("\"", "");
	}

	// ── 대기 ─────────────────────────────────────────────────────────
	/** 확인 한 번의 결과: 완료 여부와 그때의 상태(실패 메시지에 쓴다). */
	public record Poll<T>(boolean done, T state) {
	}

	/** waitUntil 의 결과: 완료 여부, 마지막 상태, 걸린 초. */
	public record Waited<T>(boolean done, T state, double elapsed) {
	}

	@FunctionalInterface
	public interface Check<T> {
		Poll<T> check() throws Exception;
	}

	/** check() 가 완료를 돌려줄 때까지(또는 timeoutSec 까지) pollSec 간격으로 다시 본다. */
	public static <T> Waited<T> waitUntil(Check<T> check, double timeoutSec, double pollSec) throws Exception {
		long started = System.nanoTime();
		while (true) {
			Poll<T> poll = check.check();
			double elapsed = (System.nanoTime() - started) / 1e9;
			if (poll.done() || elapsed >= timeoutSec)
				return new Waited<>(poll.done(), poll.state(), elapsed);
			sleepSec(pollSec);
		}
	}

	public static void sleepSec(double sec) {
		try {
			Thread.sleep((long) (sec * 1000));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("대기 중 중단됨", e);
		}
	}

	// ── 정리 ─────────────────────────────────────────────────────────
	/** 모든 버전·삭제 마커를 지우고 버킷을 삭제한다. 실패는 경고만 남긴다. */
	public static void emptyAndDeleteBucket(S3Client s3, String bucket) {
		try {
			for (var page : s3.listObjectVersionsPaginator(b -> b.bucket(bucket))) {
				List<ObjectIdentifier> targets = new ArrayList<>();
				page.versions().forEach(v -> targets.add(ObjectIdentifier.builder().key(v.key()).versionId(v.versionId()).build()));
				page.deleteMarkers().forEach(m -> targets.add(ObjectIdentifier.builder().key(m.key()).versionId(m.versionId()).build()));
				for (int i = 0; i < targets.size(); i += 1000) {
					var chunk = targets.subList(i, Math.min(i + 1000, targets.size()));
					s3.deleteObjects(b -> b.bucket(bucket).delete(Delete.builder().objects(chunk).quiet(true).build()));
				}
			}
			s3.deleteBucket(b -> b.bucket(bucket));
			Log.info("버킷 삭제: %s", bucket);
		} catch (Exception e) {
			Log.warning("버킷 삭제 실패: %s (%s)", bucket, e.getMessage());
		}
	}

	/** 이름의 시각이 days 일보다 오래된 시험 버킷을 지운다(복제 룰 → 버전·삭제 마커 → 버킷). 지운 버킷 수를 돌려준다. */
	public static int cleanupOldBuckets(S3Client s3, int days) {
		LocalDateTime cutoff = LocalDateTime.now().minusDays(days);
		List<String> names;
		try {
			names = s3.listBuckets().buckets().stream().map(b -> b.name()).toList();
		} catch (Exception e) {
			Log.warning("오래된 시험 버킷 정리: ListBuckets 실패 (%s)", e.getMessage());
			return 0;
		}
		List<String> old = names.stream().filter(n -> {
			LocalDateTime t = bucketCreatedAt(n);
			return t != null && t.isBefore(cutoff);
		}).sorted().toList();
		int deleted = 0;
		for (String bucket : old) {
			try {
				s3.deleteBucketReplication(b -> b.bucket(bucket));
			} catch (Exception e) {
				Log.warning("복제 룰 삭제 실패: %s (%s)", bucket, e.getMessage());
			}
			emptyAndDeleteBucket(s3, bucket);
			try {
				s3.headBucket(b -> b.bucket(bucket));
			} catch (Exception e) {
				deleted++;
			}
		}
		return deleted;
	}
}
