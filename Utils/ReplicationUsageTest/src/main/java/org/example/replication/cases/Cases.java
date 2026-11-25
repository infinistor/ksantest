package org.example.replication.cases;

import java.util.List;
import java.util.function.Supplier;

/** 등록된 테스트 케이스. 새 케이스는 cases/TcNN*.java 로 만들고 아래 목록에 추가한다. */
public final class Cases {
	public static final List<Supplier<ReplicationCase>> ALL = List.of(
			Tc01Basic::new, Tc02Versions::new, Tc03Prefix::new, Tc04Tag::new, Tc05PrefixTag::new,
			Tc06DeleteMarkerOff::new, Tc07DeleteMarkerOn::new, Tc08VersionDelete::new, Tc09Copy::new,
			Tc10Multipart::new, Tc11Checksum::new, Tc12Tagging::new, Tc13SseS3::new, Tc14PrefixDeleteMarker::new,
			Tc15SseSourceOnly::new, Tc16SseTargetOnly::new, Tc17MultipartChecksum::new, Tc18MultiDestination::new,
			Tc19MultiRuleMixed::new);

	private Cases() {
	}
}
