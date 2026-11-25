package org.example.replication.cases;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.example.replication.CaseFailed;
import org.example.replication.Common;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * TC17 Multipart 체크섬: 추가 체크섬을 붙여 멀티파트로 올린 객체가 체크섬 값 그대로 복제된다.
 *
 * COMPOSITE(CRC32·CRC32C·SHA1·SHA256)와 FULL_OBJECT(CRC32·CRC32C·CRC64NVME)를 모두 올린다.
 * S3Backend 복제기는 원본 HeadObject 의 헤더(체크섬 포함)를 대상 멀티파트 시작 요청에 그대로 복사하고,
 * 파트는 PartSize(기본 1GB) 단위로 다시 잘라 올린다. 대상 GW 가 시작 요청의 체크섬(객체 전체 값)을
 * 파트 값과 비교하면 파트 업로드가 400 으로 거부돼 복제가 되지 않는다.
 *
 * 원본은 Complete 응답의 체크섬이 계산값(COMPOSITE 는 base64(H(파트 체크섬 연결))-N)과 같은지 본다.
 * 대상은 복제됐는지와 내려받은 내용 MD5 만 본다. 체크섬은 보지 않는다(AWS 는 원본 값을 유지하지만
 * ksan 은 Backend HEAD 가 체크섬 헤더를 주지 않아 복제본에 체크섬이 남지 않는다). ETag 는 TC10 과 같은 이유로 보지 않는다.
 */
public class Tc17MultipartChecksum extends Tc10Multipart {
	/** (알고리즘, 체크섬 타입). CRC64NVME 는 FULL_OBJECT 만, SHA 계열은 COMPOSITE 만 된다. */
	record Combination(String algorithm, String type) {
	}

	static final List<Combination> COMBINATIONS = List.of(
			new Combination("CRC32", "COMPOSITE"),
			new Combination("CRC32C", "COMPOSITE"),
			new Combination("SHA1", "COMPOSITE"),
			new Combination("SHA256", "COMPOSITE"),
			new Combination("CRC32", "FULL_OBJECT"),
			new Combination("CRC32C", "FULL_OBJECT"),
			new Combination("CRC64NVME", "FULL_OBJECT"));

	public Tc17MultipartChecksum() {
		super("TC17", "Multipart 체크섬 복제", "체크섬 Multipart 업로드");
		compareChecksum = true;
	}

	void uploadWithChecksum(String key, String algorithm, String checksumType) throws Exception {
		String param = Checksums.param(algorithm);
		Map<String, String> meta = Map.of(META_KEY, key);
		Map<String, String> tags = sourceTags(key, null);
		String uploadId = s3.createMultipartUpload(b -> b.bucket(src).key(key).metadata(meta)
				.tagging(taggingHeader(tags))
				.checksumAlgorithm(ChecksumAlgorithm.fromValue(algorithm))
				.checksumType(ChecksumType.fromValue(checksumType))).uploadId();
		var md5s = new ByteArrayOutputStream();
		var partSums = new ByteArrayOutputStream();
		var whole = new ByteArrayOutputStream();
		List<CompletedPart> parts = new ArrayList<>();
		software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse r;
		try {
			for (int i = 0; i < PART_SIZES.length; i++) {
				int number = i + 1;
				byte[] body = Common.randomBytes(PART_SIZES[i]);
				md5s.write(Common.digest("MD5", body));
				byte[] sum = Checksums.raw(algorithm, body);
				partSums.write(sum);
				whole.write(body);
				// 값을 직접 계산해 보낸다(TC11 과 같음)
				String value = Checksums.base64(sum);
				String partEtag = s3.uploadPart(b -> {
					b.bucket(src).key(key).uploadId(uploadId).partNumber(number);
					Checksums.set(b, algorithm, value);
				}, RequestBody.fromBytes(body)).eTag();
				var part = CompletedPart.builder().partNumber(number).eTag(partEtag);
				Checksums.set(part, algorithm, value);
				parts.add(part.build());
			}
			r = s3.completeMultipartUpload(b -> b.bucket(src).key(key).uploadId(uploadId)
					.checksumType(ChecksumType.fromValue(checksumType))
					.multipartUpload(m -> m.parts(parts)));
		} catch (Exception e) {
			s3.abortMultipartUpload(b -> b.bucket(src).key(key).uploadId(uploadId));
			throw e;
		}

		String want = checksumType.equals("COMPOSITE")
				? Checksums.base64(Checksums.raw(algorithm, partSums.toByteArray())) + "-" + PART_SIZES.length
				: Checksums.base64(Checksums.raw(algorithm, whole.toByteArray()));
		Map<String, String> got = checksums(r.checksumCRC32(), r.checksumCRC32C(), r.checksumCRC64NVME(),
				r.checksumSHA1(), r.checksumSHA256());
		if (!Objects.equals(got.get(param), want))
			throw new CaseFailed(key + " Complete 응답 " + param + " 가 계산값과 다름: " + got.get(param) + " != " + want);
		String etag = Common.stripQuotes(r.eTag());
		String wantEtag = Common.md5Hex(md5s.toByteArray()) + "-" + PART_SIZES.length;
		if (!Objects.equals(etag, wantEtag))
			throw new CaseFailed(key + " Complete ETag 가 기대와 다름: " + etag + " != " + wantEtag);

		String versionId = record(key, r.versionId(), info(TOTAL_SIZE, etag, meta, tags), got);
		var kv = new VersionKey(key, versionId);
		if (dstExpected.containsKey(kv)) {
			dstExpected.get(kv).remove(ETAG);
			dstExpected.get(kv).remove(CHECKSUM);
			contentMd5.put(kv, Common.md5Hex(whole.toByteArray()));
		}
	}

	@Override
	protected void operate(List<String> detail) throws Exception {
		for (var c : COMBINATIONS)
			uploadWithChecksum("multipart-" + c.algorithm().toLowerCase() + "-" + c.type().toLowerCase(),
					c.algorithm(), c.type());
		detail.add(COMBINATIONS.size() + "개 × 파트 " + PART_SIZES.length + "개(" + TOTAL_SIZE / MIB + " MiB): "
				+ COMBINATIONS.stream().map(c -> c.algorithm() + " " + c.type()).collect(Collectors.joining(", ")));
	}
}
