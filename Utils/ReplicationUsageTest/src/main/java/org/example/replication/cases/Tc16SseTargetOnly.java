package org.example.replication.cases;

/** TC16 대상만 기본 암호화. 평문 원본이 대상 버킷 기본 암호화로 AES256 이 돼야 한다. */
public class Tc16SseTargetOnly extends Tc13SseS3 {
	public Tc16SseTargetOnly() {
		super("TC16", "SSE-S3 버킷 복제(대상만)", false, true);
	}
}
