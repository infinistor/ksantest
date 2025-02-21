package org.example.Utility;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.checksums.SdkChecksum;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

public class CheckSum {
	public static final String STR_CRC32 = "CRC32";
	public static final String STR_CRC32C = "CRC32C";
	public static final String STR_SHA1 = "SHA1";
	public static final String STR_SHA256 = "SHA256";
	public static final String STR_MD5 = "MD5";
	public static final String STR_CRC64NVME = "CRC64NVME";

	private CheckSum() {
	} // utility class이므로 인스턴스화 방지

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
		SdkChecksum hasher = SdkChecksum.forAlgorithm(S3ChecksumAlgorithm.withAlgorithm(algorithm));
		hasher.update(content.getBytes(StandardCharsets.UTF_8));
		return hasher.getChecksumBytes();
	}

	/**
	 * List<byte[]>에 대한 체크섬 bytes 반환
	 */
	public static byte[] calculateChecksumBytes(ChecksumAlgorithm algorithm, List<byte[]> contents) {
		SdkChecksum hasher = SdkChecksum.forAlgorithm(S3ChecksumAlgorithm.withAlgorithm(algorithm));
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