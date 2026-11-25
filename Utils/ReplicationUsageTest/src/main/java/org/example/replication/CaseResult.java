package org.example.replication;

/** 케이스 하나의 결과. summary.csv 한 줄이 된다. */
public class CaseResult {
	public final String caseId;
	public String result = "PASS"; // PASS / FAIL / ERROR
	public String failedStep = "";
	public String message = "";
	public Double replicationSec;
	public String srcBucket = "";
	public String dstBucket = "";
	public Usage srcUsage;
	public Usage dstUsage;

	public CaseResult(String caseId) {
		this.caseId = caseId;
	}
}
