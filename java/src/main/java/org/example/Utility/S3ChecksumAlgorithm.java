package org.example.Utility;

import java.util.Arrays;
import java.util.List;

import software.amazon.awssdk.checksums.spi.ChecksumAlgorithm;

public class S3ChecksumAlgorithm implements ChecksumAlgorithm {
	public static final String CRC32 = "CRC32";
	public static final String CRC32C = "CRC32C";
	public static final String SHA1 = "SHA1";
	public static final String SHA256 = "SHA256";
	public static final String MD5 = "MD5";
	public static final String CRC64NVME = "CRC64NVME";

	private List<String> algorithms = Arrays.asList(CRC32, CRC32C, SHA1, SHA256, MD5, CRC64NVME);

	String algorithm;

	// 알고리즘 입력
	public void fromAlgorithm(String algorithm) {
		if (!algorithms.contains(algorithm)) {
			throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
		}
		this.algorithm = algorithm;
	}

	public static ChecksumAlgorithm withAlgorithm(
			software.amazon.awssdk.services.s3.model.ChecksumAlgorithm algorithm) {
		var s3Algorithm = new S3ChecksumAlgorithm();
		switch (algorithm) {
			case CRC32 -> s3Algorithm.fromAlgorithm(CRC32);
			case CRC32_C -> s3Algorithm.fromAlgorithm(CRC32C);
			case CRC64_NVME -> s3Algorithm.fromAlgorithm(CRC64NVME);
			case SHA1 -> s3Algorithm.fromAlgorithm(SHA1);
			case SHA256 -> s3Algorithm.fromAlgorithm(SHA256);
			default -> throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
		}
		return s3Algorithm;
	}

	@Override
	public String algorithmId() {
		// AWS SDK에서 사용하는 알고리즘 ID 문자열 반환
		return algorithm;
	}
}
