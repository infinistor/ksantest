package org.example.replication;

/** 버킷 usage(파일 수·크기 합). */
public record Usage(long fileCount, long usedSize) {
	@Override
	public String toString() {
		return "count=" + fileCount + ", size=" + usedSize;
	}
}
