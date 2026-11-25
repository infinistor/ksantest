package org.example.replication;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * 복제 대상. local = 원본과 같은 시스템(리전 없는 ARN), remote = 포털에 등록된 다른 리전.
 *
 * @param s3          대상 버킷을 만들고 확인할 S3 클라이언트
 * @param region      remote 리전. local 이면 null
 * @param index       remote 리전 순번(버킷 이름 구분용)
 * @param samePortal  대상 usage 를 같은 포털로 볼 수 있는가
 */
public record Target(String kind, S3Client s3, PortalRegion region, int index, boolean samePortal) {

	public static Target local(S3Client s3) {
		return new Target("local", s3, null, 0, true);
	}

	public String caseSuffix() {
		return region == null ? "" : "@" + region.name();
	}

	public String bucketTag() {
		return region == null ? "" : "r" + index;
	}

	public String arn(String bucket) {
		return region == null ? "arn:aws:s3:::" + bucket : "arn:aws:s3:" + region.name() + "::" + bucket;
	}

	/** 대상 usage 를 볼 때 쓸 버킷 주인 키. local 이면 원본 키(null). */
	public String usageAccessKey() {
		return region == null ? null : region.accessKey();
	}

	public String usageSecretKey() {
		return region == null ? null : region.secretKey();
	}
}
