using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;

namespace s3tests.Utils
{
	/// <summary>
	/// S3 checksum helpers. Ported from java CheckSum.java.
	/// .NET AWSSDK.S3 supports CRC32, CRC32C, CRC64NVME, SHA1, SHA256.
	/// </summary>
	public static class CheckSum
	{
		public static readonly IReadOnlyList<ChecksumAlgorithm> FullObjectAlgorithms =
		[
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32C,
			ChecksumAlgorithm.CRC64NVME,
		];

		public static readonly IReadOnlyList<ChecksumAlgorithm> CompositeAlgorithms =
		[
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32C,
			ChecksumAlgorithm.SHA1,
			ChecksumAlgorithm.SHA256,
		];

		public static readonly IReadOnlyList<ChecksumAlgorithm> AllAlgorithms =
		[
			ChecksumAlgorithm.CRC32,
			ChecksumAlgorithm.CRC32C,
			ChecksumAlgorithm.CRC64NVME,
			ChecksumAlgorithm.SHA1,
			ChecksumAlgorithm.SHA256,
		];

		public static string CalculateChecksum(ChecksumAlgorithm algorithm, string content)
			=> Convert.ToBase64String(CalculateChecksumBytes(algorithm, Encoding.UTF8.GetBytes(content)));

		public static string CalculateChecksum(ChecksumAlgorithm algorithm, IList<byte[]> contents)
			=> Convert.ToBase64String(CalculateChecksumBytes(algorithm, contents)) + "-" + contents.Count;

		public static byte[] CalculateChecksumBytes(ChecksumAlgorithm algorithm, byte[] content)
		{
			if (algorithm == ChecksumAlgorithm.CRC32)
			{
				var crc = new Crc32();
				crc.Append(content);
				return ToBigEndian(crc.GetCurrentHash());
			}
			if (algorithm == ChecksumAlgorithm.CRC32C)
				return ComputeCrc32C(content);
			if (algorithm == ChecksumAlgorithm.CRC64NVME)
				return ComputeCrc64Nvme(content);
			if (algorithm == ChecksumAlgorithm.SHA1)
				return SHA1.HashData(content);
			if (algorithm == ChecksumAlgorithm.SHA256)
				return SHA256.HashData(content);

			throw new ArgumentException($"Unsupported checksum algorithm: {algorithm}");
		}

		public static byte[] CalculateChecksumBytes(ChecksumAlgorithm algorithm, string content)
			=> CalculateChecksumBytes(algorithm, Encoding.UTF8.GetBytes(content));

		public static byte[] CalculateChecksumBytes(ChecksumAlgorithm algorithm, IList<byte[]> contents)
		{
			var hasher = CreateIncremental(algorithm);
			foreach (var part in contents)
				hasher.Append(part);
			return hasher.GetHashAndReset();
		}

		public static string CalculateChecksumByBase64(ChecksumAlgorithm algorithm, IList<string> contents)
		{
			var decoded = contents.Select(Convert.FromBase64String).ToList();
			return CalculateChecksum(algorithm, decoded);
		}

		public static string CombineChecksumByBase64(ChecksumAlgorithm algorithm, long partSize, IList<string> contents)
		{
			byte[] crc1 = Convert.FromBase64String(contents[0]);
			for (int i = 1; i < contents.Count; i++)
			{
				byte[] crc2 = Convert.FromBase64String(contents[i]);
				crc1 = CrcCombine.CombineBytes(crc1, crc2, partSize, algorithm);
			}
			return Convert.ToBase64String(crc1);
		}

		public static string GetChecksum(GetObjectAttributesResponse response, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(response?.Checksum?.ChecksumCRC32, response?.Checksum?.ChecksumCRC32C,
				response?.Checksum?.ChecksumCRC64NVME, response?.Checksum?.ChecksumSHA1,
				response?.Checksum?.ChecksumSHA256, algorithm);

		public static string GetChecksum(PutObjectResponse response, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(response.ChecksumCRC32, response.ChecksumCRC32C, response.ChecksumCRC64NVME,
				response.ChecksumSHA1, response.ChecksumSHA256, algorithm);

		public static string GetChecksum(CopyObjectResponse response, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(response.ChecksumCRC32, response.ChecksumCRC32C, response.ChecksumCRC64NVME,
				response.ChecksumSHA1, response.ChecksumSHA256, algorithm);

		public static string GetChecksum(UploadPartResponse response, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(response.ChecksumCRC32, response.ChecksumCRC32C, response.ChecksumCRC64NVME,
				response.ChecksumSHA1, response.ChecksumSHA256, algorithm);

		public static string GetChecksum(CompleteMultipartUploadResponse response, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(response.ChecksumCRC32, response.ChecksumCRC32C, response.ChecksumCRC64NVME,
				response.ChecksumSHA1, response.ChecksumSHA256, algorithm);

		public static string GetChecksum(PartETag part, ChecksumAlgorithm algorithm)
			=> GetChecksumValue(part.ChecksumCRC32, part.ChecksumCRC32C, part.ChecksumCRC64NVME,
				part.ChecksumSHA1, part.ChecksumSHA256, algorithm);

		public static void SetChecksum(PutObjectRequest request, ChecksumAlgorithm algorithm, string value)
		{
			if (algorithm == ChecksumAlgorithm.CRC32) request.ChecksumCRC32 = value;
			else if (algorithm == ChecksumAlgorithm.CRC32C) request.ChecksumCRC32C = value;
			else if (algorithm == ChecksumAlgorithm.CRC64NVME) request.ChecksumCRC64NVME = value;
			else if (algorithm == ChecksumAlgorithm.SHA1) request.ChecksumSHA1 = value;
			else if (algorithm == ChecksumAlgorithm.SHA256) request.ChecksumSHA256 = value;
			else throw new ArgumentException($"Unsupported: {algorithm}");
		}

		public static void SetChecksum(UploadPartRequest request, ChecksumAlgorithm algorithm, string value)
		{
			if (algorithm == ChecksumAlgorithm.CRC32) request.ChecksumCRC32 = value;
			else if (algorithm == ChecksumAlgorithm.CRC32C) request.ChecksumCRC32C = value;
			else if (algorithm == ChecksumAlgorithm.CRC64NVME) request.ChecksumCRC64NVME = value;
			else if (algorithm == ChecksumAlgorithm.SHA1) request.ChecksumSHA1 = value;
			else if (algorithm == ChecksumAlgorithm.SHA256) request.ChecksumSHA256 = value;
			else throw new ArgumentException($"Unsupported: {algorithm}");
		}

		public static void SetChecksum(PartETag part, ChecksumAlgorithm algorithm, string value)
		{
			if (algorithm == ChecksumAlgorithm.CRC32) part.ChecksumCRC32 = value;
			else if (algorithm == ChecksumAlgorithm.CRC32C) part.ChecksumCRC32C = value;
			else if (algorithm == ChecksumAlgorithm.CRC64NVME) part.ChecksumCRC64NVME = value;
			else if (algorithm == ChecksumAlgorithm.SHA1) part.ChecksumSHA1 = value;
			else if (algorithm == ChecksumAlgorithm.SHA256) part.ChecksumSHA256 = value;
			else throw new ArgumentException($"Unsupported: {algorithm}");
		}

		public static void ApplyChecksum(PutObjectRequest request, ChecksumAlgorithm algorithm)
			=> request.ChecksumAlgorithm = algorithm;

		public static void ApplyChecksum(UploadPartRequest request, ChecksumAlgorithm algorithm)
			=> request.ChecksumAlgorithm = algorithm;

		public static void ApplyChecksum(InitiateMultipartUploadRequest request, ChecksumAlgorithm algorithm, ChecksumType checksumType = null)
		{
			request.ChecksumAlgorithm = algorithm;
			if (checksumType != null) request.ChecksumType = checksumType;
		}

		private static string GetChecksumValue(string crc32, string crc32c, string crc64, string sha1, string sha256,
			ChecksumAlgorithm algorithm)
		{
			if (algorithm == ChecksumAlgorithm.CRC32) return crc32;
			if (algorithm == ChecksumAlgorithm.CRC32C) return crc32c;
			if (algorithm == ChecksumAlgorithm.CRC64NVME) return crc64;
			if (algorithm == ChecksumAlgorithm.SHA1) return sha1;
			if (algorithm == ChecksumAlgorithm.SHA256) return sha256;
			throw new ArgumentException($"Unsupported: {algorithm}");
		}

		/// <summary>
		/// System.IO.Hashing의 CRC 계열은 해시를 리틀엔디언으로 돌려주지만
		/// S3 체크섬 헤더는 빅엔디언(네트워크 바이트 순서)을 기대한다.
		/// </summary>
		private static byte[] ToBigEndian(byte[] hash)
		{
			var bytes = (byte[])hash.Clone();
			Array.Reverse(bytes);
			return bytes;
		}

		private static byte[] ComputeCrc32C(byte[] content)
		{
			// System.IO.Hashing 9.x has Crc32 (IEEE). Use reflection for Crc32C if present.
			var crc32cType = Type.GetType("System.IO.Hashing.Crc32C, System.IO.Hashing");
			if (crc32cType != null)
			{
				dynamic instance = Activator.CreateInstance(crc32cType);
				instance.Append(content);
				return ToBigEndian((byte[])instance.GetCurrentHash());
			}
			// Fallback: Force.Crc32 package-style Castagnoli via manual if needed
			return ComputeCrc32CManual(content);
		}

		private static byte[] ComputeCrc32CManual(byte[] data)
		{
			uint crc = 0xFFFFFFFFu;
			foreach (byte b in data)
			{
				crc ^= b;
				for (int i = 0; i < 8; i++)
					crc = (crc & 1) != 0 ? (0x82F63B78u ^ (crc >> 1)) : (crc >> 1);
			}
			crc ^= 0xFFFFFFFFu;
			var bytes = BitConverter.GetBytes(crc);
			if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
			return bytes;
		}

		// CRC64-NVME polynomial 0x9A6C9329AC4BC9B5 (reflected), init/xor 0xFFFFFFFFFFFFFFFF
		private static byte[] ComputeCrc64Nvme(byte[] data)
		{
			ulong crc = 0xFFFFFFFFFFFFFFFFul;
			foreach (byte b in data)
			{
				crc ^= b;
				for (int i = 0; i < 8; i++)
					crc = (crc & 1) != 0 ? (0x9A6C9329AC4BC9B5ul ^ (crc >> 1)) : (crc >> 1);
			}
			crc ^= 0xFFFFFFFFFFFFFFFFul;
			var bytes = BitConverter.GetBytes(crc);
			if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
			return bytes;
		}

		private interface IIncrementalHash
		{
			void Append(byte[] data);
			byte[] GetHashAndReset();
		}

		private static IIncrementalHash CreateIncremental(ChecksumAlgorithm algorithm)
		{
			if (algorithm == ChecksumAlgorithm.SHA1) return new IncrementalSha1();
			if (algorithm == ChecksumAlgorithm.SHA256) return new IncrementalSha256();
			if (algorithm == ChecksumAlgorithm.CRC32) return new IncrementalCrc32();
			if (algorithm == ChecksumAlgorithm.CRC32C) return new IncrementalCrc32C();
			if (algorithm == ChecksumAlgorithm.CRC64NVME) return new IncrementalCrc64Nvme();
			throw new ArgumentException($"Unsupported: {algorithm}");
		}

		private sealed class IncrementalSha1 : IIncrementalHash
		{
			private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
			public void Append(byte[] data) => _hash.AppendData(data);
			public byte[] GetHashAndReset() => _hash.GetHashAndReset();
		}

		private sealed class IncrementalSha256 : IIncrementalHash
		{
			private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
			public void Append(byte[] data) => _hash.AppendData(data);
			public byte[] GetHashAndReset() => _hash.GetHashAndReset();
		}

		private sealed class IncrementalCrc32 : IIncrementalHash
		{
			private readonly List<byte> _buf = [];
			public void Append(byte[] data) => _buf.AddRange(data);
			public byte[] GetHashAndReset()
			{
				var crc = new Crc32();
				crc.Append(_buf.ToArray());
				_buf.Clear();
				return ToBigEndian(crc.GetCurrentHash());
			}
		}

		private sealed class IncrementalCrc32C : IIncrementalHash
		{
			private readonly List<byte> _buf = [];
			public void Append(byte[] data) => _buf.AddRange(data);
			public byte[] GetHashAndReset()
			{
				var result = ComputeCrc32C(_buf.ToArray());
				_buf.Clear();
				return result;
			}
		}

		private sealed class IncrementalCrc64Nvme : IIncrementalHash
		{
			private readonly List<byte> _buf = [];
			public void Append(byte[] data) => _buf.AddRange(data);
			public byte[] GetHashAndReset()
			{
				var result = ComputeCrc64Nvme(_buf.ToArray());
				_buf.Clear();
				return result;
			}
		}
	}
}
