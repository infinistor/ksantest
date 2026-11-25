package org.example.replication.cases;

import java.util.List;

/** TC07 삭제 마커 복제 켬: 원본에 생긴 삭제 마커가 같은 versionId 로 대상에 생긴다. 마커는 usage 에 세지 않는다. */
public class Tc07DeleteMarkerOn extends ReplicationCase {
	public Tc07DeleteMarkerOn() {
		super("TC07", "삭제 마커 복제 켬", "PUT 후 삭제(마커 생성)");
		deleteMarkerReplication = true;
	}

	@Override
	protected void operate(List<String> detail) {
		Tc06DeleteMarkerOff.putThenDelete(this, detail, "");
	}
}
