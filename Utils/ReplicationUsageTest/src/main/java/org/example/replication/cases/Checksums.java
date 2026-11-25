package org.example.replication.cases;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;

import org.example.replication.Common;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/**
 * 추가 체크섬 계산. 값을 직접 계산해 헤더로 보낸다(SDK 가 계산하게 두지 않는다).
 * 순서는 CRC32·CRC32C·CRC64NVME·SHA1·SHA256.
 */
public final class Checksums {
	private static final long CRC64NVME_POLY = 0x9A6C9329AC4BC9B5L; // 반사(reflected) 다항식
	private static final long[] CRC64NVME_TABLE = new long[256];

	static {
		for (int i = 0; i < 256; i++) {
			long c = i;
			for (int k = 0; k < 8; k++)
				c = (c & 1) != 0 ? (c >>> 1) ^ CRC64NVME_POLY : c >>> 1;
			CRC64NVME_TABLE[i] = c;
		}
	}

	/** 알고리즘 이름 → 원시 체크섬 바이트. */
	public static final Map<String, Function<byte[], byte[]>> ALGORITHMS = new LinkedHashMap<>();

	static {
		ALGORITHMS.put("CRC32", d -> {
			var c = new CRC32();
			c.update(d);
			return ByteBuffer.allocate(4).putInt((int) c.getValue()).array();
		});
		ALGORITHMS.put("CRC32C", d -> {
			var c = new CRC32C();
			c.update(d);
			return ByteBuffer.allocate(4).putInt((int) c.getValue()).array();
		});
		ALGORITHMS.put("CRC64NVME", d -> ByteBuffer.allocate(8).putLong(crc64nvme(d)).array());
		ALGORITHMS.put("SHA1", d -> Common.digest("SHA-1", d));
		ALGORITHMS.put("SHA256", d -> Common.digest("SHA-256", d));
	}

	private Checksums() {
	}

	static long crc64nvme(byte[] data) {
		long c = ~0L;
		for (byte b : data)
			c = CRC64NVME_TABLE[(int) ((c ^ b) & 0xFF)] ^ (c >>> 8);
		return ~c;
	}

	public static byte[] raw(String algorithm, byte[] data) {
		return ALGORITHMS.get(algorithm).apply(data);
	}

	public static String base64(byte[] raw) {
		return Base64.getEncoder().encodeToString(raw);
	}

	/** 응답 필드 이름(ChecksumCRC32 등). */
	public static String param(String algorithm) {
		return "Checksum" + algorithm;
	}

	public static void set(PutObjectRequest.Builder b, String algorithm, String value) {
		switch (algorithm) {
			case "CRC32" -> b.checksumCRC32(value);
			case "CRC32C" -> b.checksumCRC32C(value);
			case "CRC64NVME" -> b.checksumCRC64NVME(value);
			case "SHA1" -> b.checksumSHA1(value);
			case "SHA256" -> b.checksumSHA256(value);
			default -> throw new IllegalArgumentException(algorithm);
		}
	}

	public static void set(UploadPartRequest.Builder b, String algorithm, String value) {
		switch (algorithm) {
			case "CRC32" -> b.checksumCRC32(value);
			case "CRC32C" -> b.checksumCRC32C(value);
			case "CRC64NVME" -> b.checksumCRC64NVME(value);
			case "SHA1" -> b.checksumSHA1(value);
			case "SHA256" -> b.checksumSHA256(value);
			default -> throw new IllegalArgumentException(algorithm);
		}
	}

	public static void set(CompletedPart.Builder b, String algorithm, String value) {
		switch (algorithm) {
			case "CRC32" -> b.checksumCRC32(value);
			case "CRC32C" -> b.checksumCRC32C(value);
			case "CRC64NVME" -> b.checksumCRC64NVME(value);
			case "SHA1" -> b.checksumSHA1(value);
			case "SHA256" -> b.checksumSHA256(value);
			default -> throw new IllegalArgumentException(algorithm);
		}
	}
}
