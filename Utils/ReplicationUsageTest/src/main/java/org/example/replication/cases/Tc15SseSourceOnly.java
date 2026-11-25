package org.example.replication.cases;

/** TC15 원본만 기본 암호화. 원본 객체가 암호화돼 있으므로 복제본도 AES256 이어야 한다. */
public class Tc15SseSourceOnly extends Tc13SseS3 {
	public Tc15SseSourceOnly() {
		super("TC15", "SSE-S3 버킷 복제(원본만)", true, false);
	}
}
