package org.example.Utility;

import java.nio.ByteBuffer;
import software.amazon.awssdk.checksums.internal.CrcCombineChecksumUtil;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

/**
 * CRC(Cyclic Redundancy Check) 값을 조작하기 위한 유틸리티 클래스입니다.
 * CRC32, CRC32C, CRC64NVME 체크섬을 지원하며, 두 개의 CRC 값을 결합하는 기능을 제공합니다.
 * 주로 멀티파트 업로드나 데이터 무결성 검증에 사용됩니다.
 */
public final class CrcCombine {
	public static final int CRC32_SIZE = 32;
	public static final int CRC64_SIZE = 64;

	private static final long POLYNOMIAL_32 = 0xEDB88320L;
	private static final long[][] COMBINE_MATRICES_32 = CrcCombineChecksumUtil.generateCombineMatrices(POLYNOMIAL_32);

	private static final long POLYNOMIAL_32C = 0x82F63B78;
	private static final long[][] COMBINE_MATRICES_32C = CrcCombineChecksumUtil.generateCombineMatrices(POLYNOMIAL_32C);

	private static final long POLYNOMIAL_64 = 0x9A6C9329AC4BC9B5L;

	// GF(2) 벡터의 차원 (CRC의 길이)
	private static final int GF2_DIM = 64;

	private CrcCombine() {
	}

	private static long gf2MatrixTimes(long[] mat, long vec) {
		long sum = 0;
		int idx = 0;
		while (vec != 0) {
			if ((vec & 1) == 1)
				sum ^= mat[idx];
			vec >>>= 1;
			idx++;
		}
		return sum;
	}

	private static void gf2MatrixSquare(long[] square, long[] mat) {
		for (int n = 0; n < GF2_DIM; n++)
			square[n] = gf2MatrixTimes(mat, mat[n]);
	}

	/*
	 * 두 개의 연속된 블록의 CRC-64 값을 반환합니다.
	 * summ1은 첫 번째 블록의 CRC-64 값
	 * summ2는 두 번째 블록의 CRC-64 값
	 * len2는 두 번째 블록의 길이입니다.
	 */
	public static long crc64combine(long summ1, long summ2, long len2) {
		// 특수한 경우: 두 번째 블록의 길이가 0인 경우
		if (len2 == 0)
			return summ1;

		int n;
		long row;
		long[] even = new long[GF2_DIM]; // 2의 짝수 거듭제곱 영점 연산자
		long[] odd = new long[GF2_DIM]; // 2의 홀수 거듭제곱 영점 연산자

		// odd에 한 개의 영점 비트에 대한 연산자 설정
		odd[0] = POLYNOMIAL_64; // CRC-64 다항식

		row = 1;
		for (n = 1; n < GF2_DIM; n++) {
			odd[n] = row;
			row <<= 1;
		}

		// even에 두 개의 영점 비트에 대한 연산자 설정
		gf2MatrixSquare(even, odd);

		// odd에 네 개의 영점 비트에 대한 연산자 설정
		gf2MatrixSquare(odd, even);

		// len2개의 영점을 crc1에 적용 (첫 번째 제곱은 하나의 영점 바이트,
		// 즉 8개의 영점 비트에 대한 연산자를 even에 설정)
		long crc1 = summ1;
		long crc2 = summ2;
		do {
			// 이 len2 비트에 대한 영점 연산자 적용
			gf2MatrixSquare(even, odd);
			if ((len2 & 1) == 1)
				crc1 = gf2MatrixTimes(even, crc1);
			len2 >>>= 1;

			// 더 이상 설정된 비트가 없으면 종료
			if (len2 == 0)
				break;

			// odd와 even을 교체하여 루프 반복
			gf2MatrixSquare(odd, even);
			if ((len2 & 1) == 1)
				crc1 = gf2MatrixTimes(odd, crc1);
			len2 >>>= 1;

			// 더 이상 설정된 비트가 없으면 종료
		} while (len2 != 0);

		// 결합된 CRC 반환
		crc1 ^= crc2;
		return crc1;
	}

	public static long combine(long crc1, long crc2, int originalLengthOfCrc2, ChecksumAlgorithm type) {
		return switch (type) {
			case CRC32 -> CrcCombineChecksumUtil.combine(crc1, crc2, originalLengthOfCrc2, COMBINE_MATRICES_32);
			case CRC32_C -> CrcCombineChecksumUtil.combine(crc1, crc2, originalLengthOfCrc2, COMBINE_MATRICES_32C);
			case CRC64_NVME -> crc64combine(crc1, crc2, originalLengthOfCrc2);
			default -> throw new IllegalArgumentException("Invalid type: " + type);
		};
	}

	/**
	 * 현재 CRC64 값의 8바이트 표현을 얻습니다.
	 * 
	 * @return 8바이트로 표현된 CRC64 값
	 */
	public static byte[] longToByte(Long input) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(input);
		return buffer.array();
	}

	/**
	 * CRC 값을 4바이트 체크섬으로 변환합니다.
	 * CRC32/CRC32C의 경우 하위 4바이트만 사용됩니다.
	 */
	public static byte[] getChecksumBytes(long value) {
		byte[] valueBytes = longToByte(value);
		return new byte[] { valueBytes[4], valueBytes[5], valueBytes[6], valueBytes[7] };
	}

	/**
	 * 두 CRC 값을 결합하고 바이트 배열로 반환합니다.
	 * 
	 * @param crc1                 첫 번째 CRC 값
	 * @param crc2                 두 번째 CRC 값
	 * @param originalLengthOfCrc2 두 번째 데이터의 원본 길이
	 * @param type                 CRC 타입 (ChecksumAlgorithm.CRC32, CRC32C,
	 *                             CRC64NVME 중 하나)
	 * @return 결합된 CRC 값의 바이트 배열
	 * @throws IllegalArgumentException 유효하지 않은 CRC 타입이 지정된 경우
	 */
	public static byte[] combinebytes(long crc1, long crc2, long originalLengthOfCrc2, ChecksumAlgorithm type) {
		return switch (type) {
			case CRC32 ->
				getChecksumBytes(CrcCombineChecksumUtil.combine(crc1, crc2, originalLengthOfCrc2, COMBINE_MATRICES_32));
			case CRC32_C -> getChecksumBytes(
					CrcCombineChecksumUtil.combine(crc1, crc2, originalLengthOfCrc2, COMBINE_MATRICES_32C));
			case CRC64_NVME -> longToByte(crc64combine(crc1, crc2, originalLengthOfCrc2));
			default -> throw new IllegalArgumentException("Invalid type: " + type);
		};
	}

	/**
	 * byte[] 타입의 데이터를 long 타입으로 변환합니다.
	 * 
	 * @param bytes 변환할 바이트 배열
	 * @return 변환된 long 값
	 */
	public static long byteToInt(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.position(Long.BYTES - bytes.length); // 8바이트에서 입력 배열 길이를 뺀 위치로 이동
		buffer.put(bytes); // 나머지 공간에 입력 바이트 배열 추가
		buffer.position(0); // 위치를 처음으로 되돌림
		return buffer.getLong();
	}

	/**
	 * byte[] 타입의 데이터를 long 타입으로 변환합니다.
	 * 
	 * @param bytes 변환할 바이트 배열
	 * @return 변환된 long 값
	 */
	public static long byteToLong(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getLong();
	}

	/**
	 * 두 개의 CRC 값을 결합하고 바이트 배열로 반환합니다.
	 * 
	 * @param crc1                 첫 번째 CRC 값
	 * @param crc2                 두 번째 CRC 값
	 * @param originalLengthOfCrc2 두 번째 데이터의 원본 길이
	 * @param type                 CRC 타입 (ChecksumAlgorithm.CRC32, CRC32C,
	 *                             CRC64NVME 중 하나)
	 * @return 결합된 CRC 값의 바이트 배열
	 * @throws IllegalArgumentException 유효하지 않은 CRC 타입이 지정된 경우
	 */
	public static byte[] combinebytes(byte[] crc1, byte[] crc2, long originalLengthOfCrc2, ChecksumAlgorithm type) {
		if (type == ChecksumAlgorithm.CRC64_NVME) {
			return longToByte(crc64combine(byteToLong(crc1), byteToLong(crc2), originalLengthOfCrc2));
		}
		return combinebytes(byteToInt(crc1), byteToInt(crc2), originalLengthOfCrc2, type);
	}
}
