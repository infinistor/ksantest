using System;
using Amazon.S3;

namespace s3tests.Utils
{
	/// <summary>
	/// CRC combine for multipart FULL_OBJECT checksums (CRC32 / CRC32C / CRC64NVME).
	/// Ported from java/src/main/java/org/example/Utility/CrcCombine.java
	/// </summary>
	public static class CrcCombine
	{
		private const long Polynomial32 = 0xEDB88320L;
		private const long Polynomial32C = 0x82F63B78L;
		private const long Polynomial64 = unchecked((long)0x9A6C9329AC4BC9B5L);
		private const int Gf2Dim = 64;

		private static readonly long[][] CombineMatrices32 = GenerateCombineMatrices(Polynomial32);
		private static readonly long[][] CombineMatrices32C = GenerateCombineMatrices(Polynomial32C);

		public static byte[] CombineBytes(byte[] crc1, byte[] crc2, long originalLengthOfCrc2, ChecksumAlgorithm type)
		{
			if (type == ChecksumAlgorithm.CRC64NVME)
				return LongToBytes(Crc64Combine(BytesToLong(crc1), BytesToLong(crc2), originalLengthOfCrc2));

			return CombineBytes(BytesToInt(crc1), BytesToInt(crc2), originalLengthOfCrc2, type);
		}

		public static byte[] CombineBytes(long crc1, long crc2, long originalLengthOfCrc2, ChecksumAlgorithm type)
		{
			if (type == ChecksumAlgorithm.CRC32)
				return GetChecksumBytes32(Combine(crc1, crc2, originalLengthOfCrc2, CombineMatrices32));
			if (type == ChecksumAlgorithm.CRC32C)
				return GetChecksumBytes32(Combine(crc1, crc2, originalLengthOfCrc2, CombineMatrices32C));
			if (type == ChecksumAlgorithm.CRC64NVME)
				return LongToBytes(Crc64Combine(crc1, crc2, originalLengthOfCrc2));

			throw new ArgumentException($"Invalid type: {type}");
		}

		/// <summary>
		/// len2(바이트)만큼의 0을 crc1에 적용한 뒤 crc2와 결합한다.
		/// matrices[k]는 2^k 바이트 분량의 0 연산자이므로 len2의 set 비트 위치를 그대로 인덱스로 쓴다.
		/// </summary>
		private static long Combine(long crc1, long crc2, long len2, long[][] matrices)
		{
			long n = len2;
			long result = crc1;
			int shift = 0;
			while (n != 0)
			{
				if ((n & 1) == 1) result = Gf2MatrixTimes(matrices[shift], result);
				n = (long)((ulong)n >> 1);
				shift++;
			}
			return result ^ crc2;
		}

		/// <summary>
		/// matrices[k] = 2^k 바이트(=8·2^k 비트) 분량의 0을 적용하는 GF(2) 연산자.
		/// 1비트 연산자를 세 번 제곱하면 1바이트(8비트) 연산자가 되고, 이후 제곱할 때마다 두 배가 된다.
		/// </summary>
		private static long[][] GenerateCombineMatrices(long polynomial)
		{
			var matrices = new long[Gf2Dim][];

			var odd = new long[Gf2Dim];
			odd[0] = polynomial;
			long row = 1;
			for (int n = 1; n < Gf2Dim; n++)
			{
				odd[n] = row;
				row <<= 1;
			}

			var even = new long[Gf2Dim];
			Gf2MatrixSquare(even, odd);   // 2비트
			var cur = new long[Gf2Dim];
			Gf2MatrixSquare(cur, even);   // 4비트

			var next = new long[Gf2Dim];
			for (int n = 0; n < Gf2Dim; n++)
			{
				Gf2MatrixSquare(next, cur);   // 8비트(1바이트)부터 매번 두 배
				matrices[n] = (long[])next.Clone();
				Array.Copy(next, cur, Gf2Dim);
			}
			return matrices;
		}

		private static long Crc64Combine(long summ1, long summ2, long len2)
		{
			if (len2 == 0) return summ1;

			var even = new long[Gf2Dim];
			var odd = new long[Gf2Dim];
			odd[0] = Polynomial64;
			long row = 1;
			for (int n = 1; n < Gf2Dim; n++)
			{
				odd[n] = row;
				row <<= 1;
			}

			Gf2MatrixSquare(even, odd);
			Gf2MatrixSquare(odd, even);

			long crc1 = summ1;
			do
			{
				Gf2MatrixSquare(even, odd);
				if ((len2 & 1) == 1)
					crc1 = Gf2MatrixTimes(even, crc1);
				len2 >>= 1;
				if (len2 == 0) break;

				Gf2MatrixSquare(odd, even);
				if ((len2 & 1) == 1)
					crc1 = Gf2MatrixTimes(odd, crc1);
				len2 >>= 1;
			} while (len2 != 0);

			return crc1 ^ summ2;
		}

		private static long Gf2MatrixTimes(long[] mat, long vec)
		{
			long sum = 0;
			int idx = 0;
			while (vec != 0)
			{
				if ((vec & 1) == 1) sum ^= mat[idx];
				vec = (long)((ulong)vec >> 1);
				idx++;
			}
			return sum;
		}

		private static void Gf2MatrixSquare(long[] square, long[] mat)
		{
			for (int n = 0; n < Gf2Dim; n++)
				square[n] = Gf2MatrixTimes(mat, mat[n]);
		}

		private static byte[] LongToBytes(long input)
		{
			var buffer = BitConverter.GetBytes(input);
			if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
			return buffer;
		}

		private static byte[] GetChecksumBytes32(long value)
		{
			var valueBytes = LongToBytes(value);
			return [valueBytes[4], valueBytes[5], valueBytes[6], valueBytes[7]];
		}

		private static long BytesToInt(byte[] bytes)
		{
			var buffer = new byte[8];
			Array.Copy(bytes, 0, buffer, 8 - bytes.Length, bytes.Length);
			if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
			return BitConverter.ToInt64(buffer, 0);
		}

		private static long BytesToLong(byte[] bytes)
		{
			var buffer = (byte[])bytes.Clone();
			if (BitConverter.IsLittleEndian) Array.Reverse(buffer);
			return BitConverter.ToInt64(buffer, 0);
		}
	}
}
