package org.example.replication.cases;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.example.replication.CaseFailed;
import org.example.replication.CaseResult;
import org.example.replication.Common;
import org.example.replication.Common.Poll;
import org.example.replication.Common.Waited;
import org.example.replication.Log;
import org.example.replication.PortalClient;
import org.example.replication.Settings;
import org.example.replication.Target;
import org.example.replication.Usage;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.DeleteMarkerReplicationStatus;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ReplicationRule;
import software.amazon.awssdk.services.s3.model.ReplicationRuleAndOperator;
import software.amazon.awssdk.services.s3.model.ReplicationRuleFilter;
import software.amazon.awssdk.services.s3.model.ReplicationRuleStatus;
import software.amazon.awssdk.services.s3.model.Tag;

/**
 * 모든 케이스가 쓰는 8단계 틀: 버전 단위로 기대값을 기록하고, 대상 버전 목록·내용·usage 를 비교한다.
 *
 * 하위 케이스는 생성자에서 룰(ruleFilter·deleteMarkerReplication 등)을 정하고 operate() 만 구현한다.
 * operate() 에서 put()/record()/deleteVersion() 으로 작업하면 원본·대상 기대값이 함께 기록된다.
 */
public abstract class ReplicationCase {
	public static final String RULE_ID = "rtest-rule";
	public static final int SHOWN_ITEMS = 20; // 실패 메시지에 보여 줄 항목 수
	// 대상에 "없어야 할" 것(필터 제외·마커·버전 지정 삭제)이 늦게 도착하는지 보려고 복제 완료 뒤 더 기다리는 시간
	public static final int NEGATIVE_SETTLE_SEC = 20;
	// 원본 복제 상태(x-amz-replication-status)를 기다리는 시간·조회 간격.
	// 복제기는 대상 쓰기가 끝난 뒤 원본 상태를 갱신하므로 대상 확인보다 조금 늦을 수 있다
	public static final int STATUS_TIMEOUT_SEC = 60;
	public static final int STATUS_POLL_SEC = 3;
	public static final String STATUS_COMPLETED = "COMPLETED";
	public static final String META_KEY = "test"; // 모든 PUT 에 붙이는 사용자 메타데이터(x-amz-meta-test)
	public static final String SOURCE_TAG = "source"; // 모든 PUT 에 붙이는 기본 태그(값은 키)

	// 기대값 항목 이름. 기대값에 없는 항목은 비교하지 않는다
	public static final String SIZE = "Size";
	public static final String ETAG = "ETag";
	public static final String METADATA = "Metadata";
	public static final String TAGS = "Tags";
	public static final String CHECKSUM = "Checksum";

	private static final List<String> BASE_STEPS = List.of(
			"신규 버킷 생성",
			"버저닝 설정",
			"룰 설정",
			"작업", // 하위 클래스의 operation 으로 바뀐다
			"복제 완료까지 대기",
			"버전 비교",
			"원본 복제 상태 비교",
			"bucket usage 비교(portal api)");

	/** 룰 하나의 (Filter, 삭제 마커 복제 여부). */
	public record RuleSpec(ReplicationRuleFilter filter, boolean deleteMarker) {
	}

	// ── 하위 클래스가 생성자에서 정하는 값 ────────────────────────────
	private final String id;
	private final String title;
	private final String operation;
	protected ReplicationRuleFilter ruleFilter = prefixFilter("");
	protected boolean deleteMarkerReplication = false;
	/** 대상에 없어야 할 것이 있는 케이스면 true (복제 완료 뒤 NEGATIVE_SETTLE_SEC 더 보고 다시 확인). */
	protected boolean hasNegative = false;
	protected boolean compareChecksum = false;
	/**
	 * 대상 버킷 수. 2 이상이면 대상마다 룰을 하나씩(Priority 1, 2, ...) 건다.
	 * 룰마다 Filter·삭제 마커 복제를 다르게 하려면 ruleSpec()·replicatesTo() 를 바꾼다(TC19)
	 */
	protected int destinations = 1;

	// ── 실행 문맥 ────────────────────────────────────────────────────
	protected Settings settings;
	protected S3Client s3; // 원본 시스템 클라이언트
	protected PortalClient portal;
	protected Target target;
	protected S3Client dstS3; // 대상 버킷 클라이언트(local 이면 s3 와 같다)
	protected String caseId; // TC03, TC03@kr-ksan-1
	protected CaseResult result;
	private List<String> steps;

	protected String src = "";
	protected String dst = "";
	protected List<String> dsts = new ArrayList<>(); // 대상 버킷 전부(첫 번째가 dst)
	protected final List<String> extraBuckets = new ArrayList<>();
	// 기대값: (키, 버전) → {Size, ETag, Metadata, Tags[, Checksum]}
	protected final Map<VersionKey, Map<String, Object>> srcExpected = new LinkedHashMap<>();
	protected final Map<VersionKey, Map<String, Object>> dstExpected = new LinkedHashMap<>();
	// 대상마다의 기대값(첫 번째가 dstExpected). 대상 수는 run() 에서 정해진다
	protected List<Map<VersionKey, Map<String, Object>>> dstExpectedAll = new ArrayList<>(List.of(dstExpected));

	protected ReplicationCase(String id, String title, String operation) {
		this.id = id;
		this.title = title;
		this.operation = operation;
	}

	public String id() {
		return id;
	}

	public String title() {
		return title;
	}

	/** 실행 문맥을 채운다. 케이스 목록(--list)은 이것 없이 id()·title() 만 쓴다. */
	public ReplicationCase bind(Settings settings, S3Client s3, PortalClient portal, Target target) {
		this.settings = settings;
		this.s3 = s3;
		this.portal = portal;
		this.target = target;
		this.dstS3 = target.s3();
		this.caseId = id + target.caseSuffix();
		this.result = new CaseResult(caseId);
		this.steps = new ArrayList<>(BASE_STEPS);
		this.steps.set(3, operation);
		return this;
	}

	// ── 하위 클래스가 정하는 부분 ─────────────────────────────────────
	protected abstract void operate(List<String> detail) throws Exception;

	/** 룰 필터에 맞는 객체인지. 필터 케이스가 바꾼다. */
	protected boolean replicates(String key, Map<String, String> tags) {
		return true;
	}

	/** index 번째 대상(룰)으로 복제되는 객체인지. 대상마다 필터가 다른 케이스가 바꾼다. */
	protected boolean replicatesTo(int index, String key, Map<String, String> tags) {
		return replicates(key, tags);
	}

	/** index 번째 대상 룰의 (Filter, 삭제 마커 복제 여부). */
	protected RuleSpec ruleSpec(int index) {
		return new RuleSpec(ruleFilter, deleteMarkerReplication);
	}

	// ── 필터 ─────────────────────────────────────────────────────────
	public static ReplicationRuleFilter prefixFilter(String prefix) {
		return ReplicationRuleFilter.builder().prefix(prefix).build();
	}

	public static ReplicationRuleFilter tagFilter(Tag tag) {
		return ReplicationRuleFilter.builder().tag(tag).build();
	}

	public static ReplicationRuleFilter andFilter(String prefix, Tag... tags) {
		return ReplicationRuleFilter.builder()
				.and(ReplicationRuleAndOperator.builder().prefix(prefix).tags(tags).build()).build();
	}

	/** 빈 Filter(<Filter/>). 모든 객체가 맞는다. */
	public static ReplicationRuleFilter emptyFilter() {
		return ReplicationRuleFilter.builder().build();
	}

	// ── 작업 도우미 (원본에 작업하고 기대값을 기록) ───────────────────
	public static String taggingHeader(Map<String, String> tags) {
		return tags.entrySet().stream()
				.map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "="
						+ URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
				.collect(Collectors.joining("&"));
	}

	/** 키를 값으로 하는 기본 태그(source=키)에 extra 를 더한다. */
	public static Map<String, String> sourceTags(String key, Map<String, String> extra) {
		Map<String, String> tags = new LinkedHashMap<>();
		tags.put(SOURCE_TAG, key);
		if (extra != null)
			tags.putAll(extra);
		return tags;
	}

	/** 기대값 한 건. */
	public static Map<String, Object> info(long size, String etag, Map<String, String> meta, Map<String, String> tags) {
		Map<String, Object> info = new LinkedHashMap<>();
		info.put(SIZE, size);
		info.put(ETAG, etag);
		info.put(METADATA, meta);
		info.put(TAGS, tags);
		return info;
	}

	/** 응답의 체크섬 값들(있는 것만). 키는 ChecksumCRC32 처럼 응답 필드 이름. */
	public static Map<String, String> checksums(String crc32, String crc32c, String crc64nvme, String sha1, String sha256) {
		Map<String, String> m = new LinkedHashMap<>();
		if (crc32 != null)
			m.put("ChecksumCRC32", crc32);
		if (crc32c != null)
			m.put("ChecksumCRC32C", crc32c);
		if (crc64nvme != null)
			m.put("ChecksumCRC64NVME", crc64nvme);
		if (sha1 != null)
			m.put("ChecksumSHA1", sha1);
		if (sha256 != null)
			m.put("ChecksumSHA256", sha256);
		return m;
	}

	protected String put(String key, long size) {
		return put(key, size, null, null, null);
	}

	protected String put(String key, long size, Map<String, String> tags) {
		return put(key, size, tags, null, null);
	}

	/** 원본에 PUT 하고 기대값을 기록한다. body 가 없으면 임의 본문, extra 로 요청을 더 고친다(체크섬 등). */
	protected String put(String key, long size, Map<String, String> extraTags, Payload body,
			Consumer<PutObjectRequest.Builder> extra) {
		Payload payload = body != null ? body : Payload.random(size);
		Map<String, String> tags = sourceTags(key, extraTags);
		Map<String, String> meta = Map.of(META_KEY, key);
		var req = PutObjectRequest.builder().bucket(src).key(key).metadata(meta).tagging(taggingHeader(tags));
		if (extra != null)
			extra.accept(req);
		PutObjectResponse r = s3.putObject(req.build(), payload.requestBody());
		String etag = Common.stripQuotes(r.eTag());
		String md5 = payload.md5Hex();
		if (!Objects.equals(etag, md5))
			throw new CaseFailed(key + " 업로드 ETag 가 MD5 와 다름: " + etag + " != " + md5);
		return record(key, r.versionId(), info(size, etag, meta, tags),
				checksums(r.checksumCRC32(), r.checksumCRC32C(), r.checksumCRC64NVME(), r.checksumSHA1(),
						r.checksumSHA256()));
	}

	/** 원본 기대값과, 룰에 맞는 대상의 기대값에 기록한다. responseChecksums 는 compareChecksum 일 때만 쓴다(없으면 null). */
	protected String record(String key, String versionId, Map<String, Object> info, Map<String, String> responseChecksums) {
		if (versionId == null || versionId.isEmpty())
			throw new CaseFailed(key + " 응답에 VersionId 가 없음(버저닝이 안 걸림)");
		if (compareChecksum && responseChecksums != null)
			info.put(CHECKSUM, responseChecksums);
		var kv = new VersionKey(key, versionId);
		srcExpected.put(kv, info);
		@SuppressWarnings("unchecked")
		Map<String, String> tags = (Map<String, String>) info.get(TAGS);
		for (int index = 0; index < dstExpectedAll.size(); index++)
			if (replicatesTo(index, key, tags))
				dstExpectedAll.get(index).put(kv, new LinkedHashMap<>(info));
		return versionId;
	}

	/** 원본에서 버전을 지정해 지운다. 이 삭제는 복제되지 않으므로 대상 기대값은 그대로 둔다. */
	protected void deleteVersion(String key, String versionId) {
		s3.deleteObject(b -> b.bucket(src).key(key).versionId(versionId));
		srcExpected.remove(new VersionKey(key, versionId));
	}

	protected String newExtraBucket(String kind) {
		String bucket = bucketName(kind);
		s3.createBucket(b -> b.bucket(bucket));
		extraBuckets.add(bucket);
		return bucket;
	}

	/** 케이스마다 다른 버킷 이름(같은 초에 여러 케이스가 돌아도 겹치지 않게 케이스 ID·대상을 넣는다). */
	protected String bucketName(String kind) {
		return Common.newBucketName(id.toLowerCase() + target.bucketTag() + "-" + kind);
	}

	/** file_count·file_sizes 대로 (키, 크기) 목록을 만든다. */
	protected record SizedKey(String key, long size) {
	}

	protected List<SizedKey> sizedKeys(String prefix) {
		List<SizedKey> keys = new ArrayList<>();
		List<Long> sizes = settings.objectSizes();
		for (int i = 0; i < sizes.size(); i++)
			keys.add(new SizedKey(String.format("%sobj-%05d-%s", prefix, i, Common.formatSize(sizes.get(i))), sizes.get(i)));
		return keys;
	}

	protected List<SizedKey> sizedKeys() {
		return sizedKeys("");
	}

	// ── 실행 틀 ──────────────────────────────────────────────────────
	@FunctionalInterface
	protected interface Body {
		void run() throws Exception;
	}

	/**
	 * 단계 하나를 감싸 '[TC01][4/8] 파일 업로드 ... OK (2.1초)' 로 기록한다.
	 * detail 에 문자열을 넣으면 OK 뒤 괄호에 함께 찍는다.
	 */
	protected void step(int index, List<String> detail, Body body) throws Exception {
		String name = steps.get(index - 1);
		String tag = String.format("[%s][%d/%d] %s", caseId, index, steps.size(), name);
		long started = System.nanoTime();
		try {
			body.run();
		} catch (CaseFailed e) {
			result.failedStep = index + "/" + steps.size() + " " + name;
			Log.error("%s ... FAIL (%.1f초) %s", tag, secondsSince(started), e.getMessage());
			throw e;
		} catch (Exception e) {
			result.failedStep = index + "/" + steps.size() + " " + name;
			Log.exception(e, "%s ... ERROR (%.1f초)", tag, secondsSince(started));
			throw e;
		}
		String extra = detail == null || detail.isEmpty() ? "" : ", " + detail.get(0);
		Log.info("%s ... OK (%.1f초%s)", tag, secondsSince(started), extra);
	}

	protected void step(int index, Body body) throws Exception {
		step(index, null, body);
	}

	private static double secondsSince(long started) {
		return (System.nanoTime() - started) / 1e9;
	}

	public CaseResult execute() {
		Log.info("[%s] %s 시작", caseId, title);
		try {
			run();
		} catch (CaseFailed e) {
			result.result = "FAIL";
			result.message = e.getMessage();
		} catch (Exception e) { // 예상 못 한 오류도 결과로 남긴다
			result.result = "ERROR";
			result.message = e.getClass().getSimpleName() + ": " + e.getMessage();
		}
		if (result.result.equals("PASS") || !Common.KEEP_BUCKETS_ON_FAIL)
			cleanup();
		else
			Log.warning("[%s] 실패하여 버킷을 남김: %s, %s", caseId, result.srcBucket, result.dstBucket);
		Log.info("[%s] 결과 %s %s", caseId, result.result, result.message);
		return result;
	}

	// ── 단계 ─────────────────────────────────────────────────────────
	protected void run() throws Exception {
		src = result.srcBucket = bucketName("src");
		dsts = new ArrayList<>();
		for (int i = 0; i < destinations; i++)
			dsts.add(bucketName(i == 0 ? "dst" : "dst" + (i + 1)));
		dst = dsts.get(0);
		dstExpectedAll = new ArrayList<>();
		dstExpectedAll.add(dstExpected);
		for (int i = 1; i < dsts.size(); i++)
			dstExpectedAll.add(new LinkedHashMap<>());
		result.dstBucket = String.join(", ", dsts);

		step(1, List.of(src + ", " + String.join(", ", dsts)), () -> {
			s3.createBucket(b -> b.bucket(src));
			for (String d : dsts)
				dstS3.createBucket(b -> b.bucket(d));
		});

		step(2, () -> {
			List<String> all = new ArrayList<>(List.of(src));
			all.addAll(dsts);
			for (String bucket : all) {
				S3Client client = clientFor(bucket);
				client.putBucketVersioning(b -> b.bucket(bucket)
						.versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
				var status = client.getBucketVersioning(b -> b.bucket(bucket)).statusAsString();
				if (!"Enabled".equals(status))
					throw new CaseFailed(bucket + " 버저닝 상태가 Enabled 가 아님: " + status);
			}
		});

		step(3, () -> {
			// local 은 리전 없는 ARN, remote 는 arn:aws:s3:<리전>::<버킷>
			// 대상이 여럿이면 룰도 대상마다 하나씩 두고 Priority 를 겹치지 않게 준다
			List<ReplicationRule> rules = new ArrayList<>();
			Map<String, String> want = new LinkedHashMap<>();
			for (int i = 0; i < dsts.size(); i++) {
				String ruleId = dsts.size() == 1 ? RULE_ID : RULE_ID + "-" + (i + 1);
				RuleSpec spec = ruleSpec(i);
				String arn = target.arn(dsts.get(i));
				want.put(ruleId, arn);
				rules.add(ReplicationRule.builder()
						.id(ruleId)
						.priority(i + 1)
						.status(ReplicationRuleStatus.ENABLED)
						.filter(spec.filter())
						.destination(d -> d.bucket(arn))
						.deleteMarkerReplication(d -> d.status(spec.deleteMarker()
								? DeleteMarkerReplicationStatus.ENABLED : DeleteMarkerReplicationStatus.DISABLED))
						.build());
			}
			s3.putBucketReplication(b -> b.bucket(src).replicationConfiguration(c -> c.role("").rules(rules)));
			Map<String, String> got = new LinkedHashMap<>();
			for (var r : s3.getBucketReplication(b -> b.bucket(src)).replicationConfiguration().rules())
				got.put(r.id(), r.destination() == null ? null : r.destination().bucket());
			if (!got.equals(want))
				throw new CaseFailed("설정한 복제 룰이 그대로 조회되지 않음: 기대 " + want + " != " + got);
		});

		List<String> operateDetail = new ArrayList<>();
		step(4, operateDetail, () -> {
			operate(operateDetail);
			if (operateDetail.isEmpty())
				operateDetail.add("원본 " + usageOf(srcExpected) + ", 대상 기대 " + usageOf(dstExpected));
		});

		List<String> waitDetail = new ArrayList<>();
		step(5, waitDetail, () -> {
			double elapsed = waitReplicated();
			result.replicationSec = Math.round(elapsed * 10) / 10.0;
			waitDetail.add(String.format("복제 %.1f초", elapsed));
			if (hasNegative) {
				Common.sleepSec(NEGATIVE_SETTLE_SEC);
				Poll<String> poll = dstMatches();
				if (!poll.done())
					throw new CaseFailed("복제 완료 " + NEGATIVE_SETTLE_SEC + "초 뒤 대상이 달라짐: " + poll.state());
				waitDetail.set(0, waitDetail.get(0) + ", 이후 " + NEGATIVE_SETTLE_SEC + "초 변화 없음");
			}
		});

		step(6, this::compareVersions);

		List<String> statusDetail = new ArrayList<>();
		step(7, statusDetail, () -> compareReplicationStatus(statusDetail));

		List<String> usageDetail = new ArrayList<>();
		step(8, usageDetail, () -> compareUsage(usageDetail));
	}

	// ── 비교 ─────────────────────────────────────────────────────────
	/** 버킷의 파일 버전 {(키, 버전): {Size, ETag}} 과 삭제 마커 {(키, 버전)}. */
	public record Versions(Map<VersionKey, Map<String, Object>> files, Set<VersionKey> markers) {
	}

	public static Versions listVersions(S3Client client, String bucket) {
		Map<VersionKey, Map<String, Object>> files = new LinkedHashMap<>();
		Set<VersionKey> markers = new HashSet<>();
		for (var page : client.listObjectVersionsPaginator(b -> b.bucket(bucket))) {
			for (var v : page.versions()) {
				Map<String, Object> m = new LinkedHashMap<>();
				m.put(SIZE, v.size());
				m.put(ETAG, Common.stripQuotes(v.eTag()));
				files.put(new VersionKey(v.key(), v.versionId()), m);
			}
			for (var m : page.deleteMarkers())
				markers.add(new VersionKey(m.key(), m.versionId()));
		}
		return new Versions(files, markers);
	}

	public static Map<String, String> getTags(S3Client client, String bucket, String key, String versionId) {
		Map<String, String> tags = new HashMap<>();
		for (var t : client.getObjectTagging(b -> b.bucket(bucket).key(key).versionId(versionId)).tagSet())
			tags.put(t.key(), t.value());
		return tags;
	}

	/** 파일 버전만 센다. 삭제 마커는 usage 에 반영되지 않는다. */
	public static Usage usageOf(Map<VersionKey, Map<String, Object>> versions) {
		long size = 0;
		for (var v : versions.values())
			size += (Long) v.get(SIZE);
		return new Usage(versions.size(), size);
	}

	/** 정렬해 앞의 SHOWN_ITEMS 개만. */
	public static <T extends Comparable<T>> List<T> shortList(Set<T> items) {
		return items.stream().sorted().limit(SHOWN_ITEMS).toList();
	}

	public static <T> List<T> head(List<T> items) {
		return items.subList(0, Math.min(items.size(), SHOWN_ITEMS));
	}

	/** index 번째 대상에 있어야 할 삭제 마커. 태그 필터 룰은 삭제 마커 복제를 켤 수 없어 태그는 보지 않는다. */
	protected Set<VersionKey> expectedMarkers(Set<VersionKey> srcMarkers, int index) {
		Set<VersionKey> want = new HashSet<>();
		if (!ruleSpec(index).deleteMarker())
			return want;
		for (VersionKey kv : srcMarkers)
			if (replicatesTo(index, kv.key(), Map.of()))
				want.add(kv);
		return want;
	}

	protected Poll<String> dstMatches() {
		Set<VersionKey> srcMarkers = listVersions(s3, src).markers();
		List<String> diffs = new ArrayList<>();
		for (int index = 0; index < dsts.size(); index++) {
			String d = dsts.get(index);
			Poll<String> one = oneDstMatches(d, dstExpectedAll.get(index), expectedMarkers(srcMarkers, index));
			if (!one.done())
				diffs.add(dsts.size() == 1 ? one.state() : d + ": " + one.state());
		}
		return new Poll<>(diffs.isEmpty(), String.join(" // ", diffs));
	}

	private Poll<String> oneDstMatches(String bucket, Map<VersionKey, Map<String, Object>> expected,
			Set<VersionKey> wantMarkers) {
		Versions got = listVersions(dstS3, bucket);
		Set<VersionKey> missing = minus(expected.keySet(), got.files().keySet());
		Set<VersionKey> extra = minus(got.files().keySet(), expected.keySet());
		Set<VersionKey> missingM = minus(wantMarkers, got.markers());
		Set<VersionKey> extraM = minus(got.markers(), wantMarkers);
		if (missing.isEmpty() && extra.isEmpty() && missingM.isEmpty() && extraM.isEmpty())
			return new Poll<>(true, "");
		List<String> parts = new ArrayList<>();
		if (!missing.isEmpty())
			parts.add("빠진 버전 " + missing.size() + "개 " + shortList(missing));
		if (!extra.isEmpty())
			parts.add("남는 버전 " + extra.size() + "개 " + shortList(extra));
		if (!missingM.isEmpty())
			parts.add("빠진 삭제 마커 " + missingM.size() + "개 " + shortList(missingM));
		if (!extraM.isEmpty())
			parts.add("남는 삭제 마커 " + extraM.size() + "개 " + shortList(extraM));
		return new Poll<>(false, String.join(" / ", parts));
	}

	protected static <T> Set<T> minus(Set<T> a, Set<T> b) {
		Set<T> r = new HashSet<>(a);
		r.removeAll(b);
		return r;
	}

	/** 대상이 기대와 같아질 때까지 기다리고 걸린 초를 돌려준다. */
	protected double waitReplicated() throws Exception {
		Waited<String> w = Common.waitUntil(this::dstMatches, Common.REPLICATION_TIMEOUT_SEC, Common.REPLICATION_POLL_SEC);
		if (!w.done())
			throw new CaseFailed(Common.REPLICATION_TIMEOUT_SEC + "초 안에 대상이 기대와 같아지지 않음: " + w.state());
		return w.elapsed();
	}

	/** 대상 버킷이면 대상 클라이언트, 나머지(원본·보조 버킷)는 원본 클라이언트. */
	protected S3Client clientFor(String bucket) {
		return dsts.contains(bucket) ? dstS3 : s3;
	}

	protected Map<String, Object> describe(String bucket, String key, String versionId) {
		S3Client client = clientFor(bucket);
		HeadObjectResponse h = client.headObject(b -> {
			b.bucket(bucket).key(key).versionId(versionId);
			if (compareChecksum)
				b.checksumMode(ChecksumMode.ENABLED);
		});
		// 사용자 메타데이터 키는 대소문자를 가리지 않으므로 소문자로 맞춘다
		Map<String, String> meta = new HashMap<>();
		h.metadata().forEach((k, v) -> meta.put(k.toLowerCase(), v));
		Map<String, Object> info = info(h.contentLength(), Common.stripQuotes(h.eTag()), meta,
				getTags(client, bucket, key, versionId));
		if (compareChecksum)
			info.put(CHECKSUM, checksums(h.checksumCRC32(), h.checksumCRC32C(), h.checksumCRC64NVME(),
					h.checksumSHA1(), h.checksumSHA256()));
		return info;
	}

	protected void compareVersions() {
		var srcFiles = listVersions(s3, src).files().keySet();
		List<String> diffs = new ArrayList<>();
		if (!srcFiles.equals(srcExpected.keySet()))
			diffs.add("원본 버전 목록이 기대와 다름: 빠짐 " + shortList(minus(srcExpected.keySet(), srcFiles))
					+ ", 남음 " + shortList(minus(srcFiles, srcExpected.keySet())));
		List<String> buckets = new ArrayList<>(List.of(src));
		buckets.addAll(dsts);
		List<Map<VersionKey, Map<String, Object>>> expectations = new ArrayList<>(List.of(srcExpected));
		expectations.addAll(dstExpectedAll);
		for (int i = 0; i < buckets.size(); i++) {
			String bucket = buckets.get(i);
			for (var e : expectations.get(i).entrySet()) {
				VersionKey kv = e.getKey();
				Map<String, Object> got = describe(bucket, kv.key(), kv.versionId());
				for (var field : e.getValue().entrySet())
					if (!Objects.equals(got.get(field.getKey()), field.getValue()))
						diffs.add(bucket + "/" + kv.key() + "?versionId=" + kv.versionId() + " " + field.getKey()
								+ ": 기대 " + field.getValue() + " != " + got.get(field.getKey()));
			}
		}
		if (!diffs.isEmpty())
			throw new CaseFailed("버전 불일치 " + diffs.size() + "건: " + head(diffs));
	}

	/**
	 * 원본 파일 버전마다 HeadObject 의 ReplicationStatus 기대값.
	 * 룰에 맞아 복제된 버전은 COMPLETED, 룰에 맞지 않는 버전은 헤더가 없어야 한다(AWS 와 같음).
	 * 대상이 여럿이면 맞는 대상 모두에 복제가 끝나야 COMPLETED 다.
	 */
	protected Map<VersionKey, String> expectedReplicationStatus() {
		Map<VersionKey, String> want = new LinkedHashMap<>();
		for (VersionKey kv : srcExpected.keySet())
			want.put(kv, dstExpectedAll.stream().anyMatch(e -> e.containsKey(kv)) ? STATUS_COMPLETED : null);
		return want;
	}

	protected void compareReplicationStatus(List<String> detail) throws Exception {
		Map<VersionKey, String> expected = expectedReplicationStatus();
		Waited<List<String>> w = Common.waitUntil(() -> {
			List<String> diffs = new ArrayList<>();
			for (var e : expected.entrySet()) {
				VersionKey kv = e.getKey();
				String got = s3.headObject(b -> b.bucket(src).key(kv.key()).versionId(kv.versionId()))
						.replicationStatusAsString();
				if (!Objects.equals(got, e.getValue()))
					diffs.add(src + "/" + kv.key() + "?versionId=" + kv.versionId() + " ReplicationStatus: 기대 "
							+ e.getValue() + " != " + got);
			}
			return new Poll<>(diffs.isEmpty(), diffs);
		}, STATUS_TIMEOUT_SEC, STATUS_POLL_SEC);
		if (!w.done())
			throw new CaseFailed(STATUS_TIMEOUT_SEC + "초 안에 원본 복제 상태가 맞지 않음 " + w.state().size() + "건: "
					+ head(w.state()));
		long completed = expected.values().stream().filter(STATUS_COMPLETED::equals).count();
		detail.add(String.format("%s %d개, 상태 없음 %d개, 수렴 %.1f초", STATUS_COMPLETED, completed,
				expected.size() - completed, w.elapsed()));
	}

	protected void compareUsage(List<String> detail) throws Exception {
		Usage wantSrc = usageOf(srcExpected);
		List<Usage> wants = dstExpectedAll.stream().map(ReplicationCase::usageOf).toList();
		// 대상이 하나면 그 값, 여럿이면 대상별 목록으로 보인다
		Object wantDst = wants.size() == 1 ? wants.get(0) : wants;
		// 다른 시스템 리전이면 대상 usage 는 이 포털로 볼 수 없어 원본만 비교한다
		boolean checkDst = target.samePortal();

		Waited<Object[]> w = Common.waitUntil(() -> {
			Usage s = portal.bucketUsage(src);
			List<Usage> ds = new ArrayList<>();
			if (checkDst)
				for (String d : dsts)
					ds.add(portal.bucketUsage(d, target.usageAccessKey(), target.usageSecretKey()));
			// 결과(CSV)에는 첫 대상 값을 남긴다
			result.srcUsage = s;
			result.dstUsage = ds.isEmpty() ? null : ds.get(0);
			Object d = ds.isEmpty() ? null : ds.size() == 1 ? ds.get(0) : ds;
			return new Poll<>(s.equals(wantSrc) && (ds.isEmpty() || ds.equals(wants)), new Object[] { s, d });
		}, Common.USAGE_TIMEOUT_SEC, Common.USAGE_POLL_SEC);
		if (!w.done())
			throw new CaseFailed(Common.USAGE_TIMEOUT_SEC + "초 안에 usage 가 맞지 않음: src 기대 " + wantSrc + " 실제 "
					+ w.state()[0] + " / dst 기대 " + wantDst + " 실제 " + w.state()[1]);
		if (!checkDst) {
			detail.add(String.format("src %s, dst 는 다른 포털이라 건너뜀, 수렴 %.1f초", wantSrc, w.elapsed()));
			return;
		}
		String same = wantSrc.equals(wantDst) ? "src=dst " : "";
		detail.add(String.format("%ssrc %s, dst %s, 수렴 %.1f초", same, wantSrc, wantDst, w.elapsed()));
	}

	public void cleanup() {
		if (!src.isEmpty()) {
			// 복제 룰을 먼저 지워야 원본 삭제가 대상에 다시 복제되지 않는다
			try {
				s3.deleteBucketReplication(b -> b.bucket(src));
			} catch (Exception e) {
				Log.warning("복제 룰 삭제 실패: %s (%s)", src, e.getMessage());
			}
		}
		List<String> all = new ArrayList<>();
		all.add(src);
		all.addAll(dsts);
		all.addAll(extraBuckets);
		for (String bucket : all)
			if (!bucket.isEmpty())
				Common.emptyAndDeleteBucket(clientFor(bucket), bucket);
	}
}
