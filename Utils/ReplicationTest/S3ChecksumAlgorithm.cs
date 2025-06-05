namespace ReplicationTest
{
	public enum S3ChecksumAlgorithm
	{
		None,
		CRC32,
		CRC32C,
		CRC64NVME,
		SHA1,
		SHA256,
	}

	public static class S3ChecksumAlgorithmExtensions
	{
		public static string ToName(this S3ChecksumAlgorithm algorithm) => algorithm switch
		{
			S3ChecksumAlgorithm.CRC32 => "CRC32",
			S3ChecksumAlgorithm.CRC32C => "CRC32C",
			S3ChecksumAlgorithm.CRC64NVME => "CRC64NVME",
			S3ChecksumAlgorithm.SHA1 => "SHA1",
			S3ChecksumAlgorithm.SHA256 => "SHA256",
			_ => "None",
		};
	}
}
