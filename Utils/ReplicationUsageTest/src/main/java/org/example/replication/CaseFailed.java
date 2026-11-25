package org.example.replication;

/** 케이스 판정 실패. 단계 번호와 이름은 ReplicationCase.step 이 채운다. */
public class CaseFailed extends RuntimeException {
	public CaseFailed(String message) {
		super(message);
	}

	public CaseFailed(String message, Throwable cause) {
		super(message, cause);
	}
}
