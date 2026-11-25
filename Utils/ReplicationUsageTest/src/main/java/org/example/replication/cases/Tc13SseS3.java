package org.example.replication.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.example.replication.CaseFailed;

import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * SSE-S3 기본 암호화 조합 케이스.
 *
 * - TC13: 원본·대상 둘 다 기본 암호화(AES256)
 * - TC15: 원본만 기본 암호화 → 원본 객체가 암호화돼 있으므로 복제본도 암호화돼야 한다
 * - TC16: 대상만 기본 암호화 → 평문 원본이 대상 버킷 기본 암호화로 암호화돼야 한다
 */
public class Tc13SseS3 extends ReplicationCase {
	static final String SSE_ALGORITHM = "AES256";

	private final boolean srcEncrypted; // 원본 버킷에 기본 암호화를 거는가
	private final boolean dstEncrypted; // 대상 버킷에 기본 암호화를 거는가

	public Tc13SseS3() {
		this("TC13", "SSE-S3 버킷 복제(원본·대상)", true, true);
	}

	protected Tc13SseS3(String id, String title, boolean srcEncrypted, boolean dstEncrypted) {
		super(id, title, "기본 암호화 설정 후 PUT");
		this.srcEncrypted = srcEncrypted;
		this.dstEncrypted = dstEncrypted;
	}

	@Override
	protected void operate(List<String> detail) {
		for (var e : Map.of(src, srcEncrypted, dst, dstEncrypted).entrySet()) {
			String bucket = e.getKey();
			if (e.getValue())
				clientFor(bucket).putBucketEncryption(b -> b.bucket(bucket).serverSideEncryptionConfiguration(c -> c
						.rules(r -> r.applyServerSideEncryptionByDefault(d -> d.sseAlgorithm(ServerSideEncryption.AES256)))));
		}
		for (var k : sizedKeys())
			put(k.key(), k.size());
		detail.add("원본 암호화 " + srcEncrypted + ", 대상 암호화 " + dstEncrypted);
	}

	@Override
	protected void compareVersions() {
		super.compareVersions();
		// 원본 객체는 원본 버킷 설정을 따르고, 복제본은 원본이 암호화됐거나 대상 버킷이 암호화면 암호화
		String wantSrc = srcEncrypted ? SSE_ALGORITHM : null;
		String wantDst = srcEncrypted || dstEncrypted ? SSE_ALGORITHM : null;
		List<String> wrong = new ArrayList<>();
		check(src, srcExpected, wantSrc, wrong);
		check(dst, dstExpected, wantDst, wrong);
		if (!wrong.isEmpty())
			throw new CaseFailed("암호화 상태가 다른 버전 " + wrong.size() + "개: " + head(wrong));
	}

	private void check(String bucket, Map<VersionKey, Map<String, Object>> expected, String want, List<String> wrong) {
		var client = clientFor(bucket);
		for (VersionKey kv : expected.keySet()) {
			String sse = client.headObject(b -> b.bucket(bucket).key(kv.key()).versionId(kv.versionId()))
					.serverSideEncryptionAsString();
			if (!Objects.equals(sse, want))
				wrong.add(bucket + "/" + kv.key() + " 기대 " + want + " != " + sse);
		}
	}
}
