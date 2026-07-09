package org.example.Utility;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm;
import software.amazon.awssdk.checksums.SdkChecksum;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

public class CheckSum {

	private CheckSum() {
	} // utility class이므로 인스턴스화 방지

	/**
	 * FULL_OBJECT 타입을 지원하는 체크섬 알고리즘 목록 (CRC 계열만 결합 가능)
	 */
	public static final List<ChecksumAlgorithm> FULL_OBJECT_ALGORITHMS = List.of(
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32_C,
			ChecksumAlgorithm.CRC64_NVME);

	/**
	 * COMPOSITE 타입을 지원하는 체크섬 알고리즘 목록 (CRC64NVME 제외)
	 */
	public static final List<ChecksumAlgorithm> COMPOSITE_ALGORITHMS = List.of(
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32_C,
			ChecksumAlgorithm.SHA1,
			ChecksumAlgorithm.SHA256,
			ChecksumAlgorithm.MD5,
			ChecksumAlgorithm.SHA512,
			ChecksumAlgorithm.XXHASH64,
			ChecksumAlgorithm.XXHASH3,
			ChecksumAlgorithm.XXHASH128);

	/**
	 * 모든 체크섬 알고리즘 목록 (PutObject, UploadPart 파트 체크섬에서 사용 가능)
	 */
	public static final List<ChecksumAlgorithm> ALL_ALGORITHMS = List.of(
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32_C,
			ChecksumAlgorithm.CRC64_NVME,
			ChecksumAlgorithm.SHA1,
			ChecksumAlgorithm.SHA256,
			ChecksumAlgorithm.MD5,
			ChecksumAlgorithm.SHA512,
			ChecksumAlgorithm.XXHASH64,
			ChecksumAlgorithm.XXHASH3,
			ChecksumAlgorithm.XXHASH128);

	/**
	 * 문자열에 대한 체크섬 계산
	 */
	public static String calculateChecksum(ChecksumAlgorithm algorithm, String content) {
		return Base64.getEncoder().encodeToString(calculateChecksumBytes(algorithm, content));
	}

	/**
	 * List<byte[]>에 대한 체크섬 계산
	 */
	public static String calculateChecksum(ChecksumAlgorithm algorithm, List<byte[]> contents) {
		return Base64.getEncoder().encodeToString(calculateChecksumBytes(algorithm, contents)) + "-" + contents.size();
	}

	/**
	 * 문자열에 대한 체크섬 bytes 반환
	 */
	public static byte[] calculateChecksumBytes(ChecksumAlgorithm algorithm, String content) {
		SdkChecksum hasher = SdkChecksum.forAlgorithm(DefaultChecksumAlgorithm.fromValue(algorithm.toString()));
		hasher.update(content.getBytes(StandardCharsets.UTF_8));
		return hasher.getChecksumBytes();
	}

	/**
	 * List<byte[]>에 대한 체크섬 bytes 반환
	 */
	public static byte[] calculateChecksumBytes(ChecksumAlgorithm algorithm, List<byte[]> contents) {
		SdkChecksum hasher = SdkChecksum.forAlgorithm(DefaultChecksumAlgorithm.fromValue(algorithm.toString()));
		for (byte[] content : contents) {
			hasher.update(content);
		}
		return hasher.getChecksumBytes();
	}

	/**
	 * List<String>에 대한 체크섬 계산
	 */
	public static String calculateChecksumByBase64(ChecksumAlgorithm algorithm, List<String> contents) {
		return calculateChecksum(algorithm, contents.stream().map(Base64.getDecoder()::decode).toList());
	}

	/**
	 * List<String>에 대한 체크섬 계산
	 */
	public static String combineChecksumByBase64(ChecksumAlgorithm algorithm, long partSize, List<String> contents) {
		byte[] crc1 = Base64.getDecoder().decode(contents.get(0));

		for (var index = 1; index < contents.size(); index++) {
			byte[] crc2 = Base64.getDecoder().decode(contents.get(index));
			crc1 = CrcCombine.combinebytes(crc1, crc2, partSize, algorithm);
		}
		return Base64.getEncoder().encodeToString(crc1);
	}

	/**
	 * 응답 및 모델 객체(PutObjectResponse, UploadPartResponse, CompletedPart,
	 * CopyObjectResult 등)에서 지정한 알고리즘의 체크섬 값을 조회
	 */
	public static String getChecksum(SdkPojo pojo, ChecksumAlgorithm algorithm) {
		return (String) checksumField(pojo, algorithm).getValueOrDefault(pojo);
	}

	/**
	 * 리퀘스트/모델 빌더(CompletedPart.Builder, PutObjectRequest.Builder 등)에
	 * 지정한 알고리즘의 체크섬 값을 설정
	 */
	public static void setChecksum(SdkPojo builder, ChecksumAlgorithm algorithm, String value) {
		checksumField(builder, algorithm).set(builder, value);
	}

	/**
	 * 업로드 리퀘스트 빌더(PutObjectRequest.Builder, UploadPartRequest.Builder 등)에
	 * 체크섬 알고리즘을 지정. 단 SDK가 MD5의 자동 계산을 거부하므로
	 * ("MD5 is not supported. Please use a pre-calculated MD5 value")
	 * MD5는 본문으로 사전 계산한 체크섬 값을 대신 지정한다.
	 */
	public static void applyChecksum(SdkPojo builder, ChecksumAlgorithm algorithm, String content) {
		if (algorithm == ChecksumAlgorithm.MD5) {
			setChecksum(builder, algorithm, calculateChecksum(algorithm, content));
		} else {
			findField(builder, "ChecksumAlgorithm").set(builder, algorithm.toString());
		}
	}

	private static SdkField<?> checksumField(SdkPojo pojo, ChecksumAlgorithm algorithm) {
		return findField(pojo, "Checksum" + algorithm);
	}

	private static SdkField<?> findField(SdkPojo pojo, String memberName) {
		for (SdkField<?> field : pojo.sdkFields()) {
			if (field.memberName().equals(memberName)) {
				return field;
			}
		}
		throw new IllegalArgumentException("Field not found: " + memberName);
	}

	/**
	 * List<byte[]>에 대한 CRC32 체크섬 계산
	 */
	public static String crc32(List<byte[]> contents) {
		return calculateChecksum(ChecksumAlgorithm.CRC32, contents);
	}

	/**
	 * List<byte[]>에 대한 CRC32C 체크섬 계산
	 */
	public static String crc32c(List<byte[]> contents) {
		return calculateChecksum(ChecksumAlgorithm.CRC32_C, contents);
	}

	/**
	 * List<byte[]>에 대한 CRC64 체크섬 계산
	 */
	public static String crc64Nvme(List<byte[]> contents) {
		return calculateChecksum(ChecksumAlgorithm.CRC64_NVME, contents);
	}

	/**
	 * List<byte[]>에 대한 SHA1 체크섬 계산
	 */
	public static String sha1(List<byte[]> contents) {
		return calculateChecksum(ChecksumAlgorithm.SHA1, contents);
	}

	/**
	 * List<byte[]>에 대한 SHA256 체크섬 계산
	 */
	public static String sha256(List<byte[]> contents) {
		return calculateChecksum(ChecksumAlgorithm.SHA256, contents);
	}
}
