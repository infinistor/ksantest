package org.example.replication.cases;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.example.replication.CaseFailed;
import org.example.replication.Common;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * TC10 Multipart: 멀티파트로 올린 객체가 크기·내용·메타데이터·태그 그대로 복제된다.
 *
 * 원본은 Complete ETag 가 md5(파트 md5 연결)-N 인지 본다.
 * 대상 ETag 는 비교하지 않는다. S3Backend 복제기는 원본 파트 경계가 아니라 PartSize(기본 1GB) 단위로
 * 다시 잘라 올리므로 파트 수가 달라지면 ETag 가 달라진다(AWS 는 원본 파트 구성을 유지한다).
 * 대신 대상 객체를 내려받아 전체 MD5 를 원본과 비교한다.
 */
public class Tc10Multipart extends ReplicationCase {
	static final int MIB = 1024 * 1024;
	static final int[] PART_SIZES = { 5 * MIB, 5 * MIB, 1 * MIB }; // 마지막 파트만 5 MiB 미만 허용
	static final long TOTAL_SIZE = 11L * MIB;
	static final int MULTIPART_OBJECTS = 2;
	static final int CHUNK_SIZE = 1 * MIB; // 대상 내용 MD5 를 계산할 때 읽는 단위

	/** 대상 (키, 버전) → 원본 전체 MD5. */
	protected final Map<VersionKey, String> contentMd5 = new LinkedHashMap<>();

	public Tc10Multipart() {
		this("TC10", "Multipart 복제", "Multipart 업로드");
	}

	protected Tc10Multipart(String id, String title, String operation) {
		super(id, title, operation);
	}

	void upload(String key) throws Exception {
		Map<String, String> meta = Map.of(META_KEY, key);
		Map<String, String> tags = sourceTags(key, null);
		String uploadId = s3.createMultipartUpload(b -> b.bucket(src).key(key).metadata(meta)
				.tagging(taggingHeader(tags))).uploadId();
		var whole = MessageDigest.getInstance("MD5");
		var digests = new ByteArrayOutputStream();
		List<CompletedPart> parts = new ArrayList<>();
		String etag;
		String versionId;
		try {
			for (int i = 0; i < PART_SIZES.length; i++) {
				int number = i + 1;
				byte[] body = Common.randomBytes(PART_SIZES[i]);
				digests.write(Common.digest("MD5", body));
				whole.update(body);
				String partEtag = s3.uploadPart(b -> b.bucket(src).key(key).uploadId(uploadId).partNumber(number),
						RequestBody.fromBytes(body)).eTag();
				parts.add(CompletedPart.builder().partNumber(number).eTag(partEtag).build());
			}
			var r = s3.completeMultipartUpload(b -> b.bucket(src).key(key).uploadId(uploadId)
					.multipartUpload(m -> m.parts(parts)));
			etag = Common.stripQuotes(r.eTag());
			versionId = r.versionId();
		} catch (Exception e) {
			s3.abortMultipartUpload(b -> b.bucket(src).key(key).uploadId(uploadId));
			throw e;
		}
		String want = Common.md5Hex(digests.toByteArray()) + "-" + PART_SIZES.length;
		if (!Objects.equals(etag, want))
			throw new CaseFailed(key + " Complete ETag 가 기대와 다름: " + etag + " != " + want);
		versionId = record(key, versionId, info(TOTAL_SIZE, etag, meta, tags), null);
		// 대상은 ETag 대신 내용 MD5 로 본다
		var kv = new VersionKey(key, versionId);
		if (dstExpected.containsKey(kv)) {
			dstExpected.get(kv).remove(ETAG);
			contentMd5.put(kv, HexFormat.of().formatHex(whole.digest()));
		}
	}

	@Override
	protected void compareVersions() {
		super.compareVersions();
		List<String> diffs = new ArrayList<>();
		for (var e : contentMd5.entrySet()) {
			VersionKey kv = e.getKey();
			String got = downloadMd5(kv);
			if (!got.equals(e.getValue()))
				diffs.add(dst + "/" + kv.key() + "?versionId=" + kv.versionId() + " 내용 MD5: 기대 " + e.getValue()
						+ " != " + got);
		}
		if (!diffs.isEmpty())
			throw new CaseFailed("대상 내용 불일치 " + diffs.size() + "건: " + diffs);
	}

	private String downloadMd5(VersionKey kv) {
		try (InputStream body = dstS3.getObject(b -> b.bucket(dst).key(kv.key()).versionId(kv.versionId()))) {
			var md5 = MessageDigest.getInstance("MD5");
			byte[] buf = new byte[CHUNK_SIZE];
			int n;
			while ((n = body.read(buf)) > 0)
				md5.update(buf, 0, n);
			return HexFormat.of().formatHex(md5.digest());
		} catch (Exception e) {
			throw new IllegalStateException(dst + "/" + kv.key() + " 내려받기 실패: " + e.getMessage(), e);
		}
	}

	@Override
	protected void operate(List<String> detail) throws Exception {
		for (int i = 0; i < MULTIPART_OBJECTS; i++)
			upload(String.format("multipart-%05d", i));
		detail.add(MULTIPART_OBJECTS + "개 × 파트 " + PART_SIZES.length + "개(" + TOTAL_SIZE / MIB + " MiB)");
	}
}
