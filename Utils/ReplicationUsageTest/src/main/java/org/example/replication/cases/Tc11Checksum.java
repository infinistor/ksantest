package org.example.replication.cases;

import java.util.List;
import java.util.Objects;

import org.example.replication.CaseFailed;
import org.example.replication.Common;

/**
 * TC11 체크섬: 추가 체크섬(CRC32·CRC32C·CRC64NVME·SHA1·SHA256)을 붙여 올린 객체의 체크섬 값이 대상에서도 같다.
 * 값은 직접 계산해 헤더로 보낸다.
 */
public class Tc11Checksum extends ReplicationCase {
	public Tc11Checksum() {
		super("TC11", "체크섬 복제", "추가 체크섬 5종 PUT");
		compareChecksum = true;
	}

	@Override
	protected void operate(List<String> detail) {
		List<Long> sizes = settings.objectSizes();
		int i = 0;
		for (String name : Checksums.ALGORITHMS.keySet()) {
			long size = sizes.get(i++ % sizes.size());
			if (size > Integer.MAX_VALUE)
				throw new CaseFailed("TC11 은 본문을 메모리에서 계산하므로 2G 미만 크기만 쓴다: " + Common.formatSize(size));
			byte[] body = Common.randomBytes((int) size);
			String value = Checksums.base64(Checksums.raw(name, body));
			String param = Checksums.param(name);
			String key = "checksum-" + name.toLowerCase();
			String versionId = put(key, size, null, Payload.of(body), b -> Checksums.set(b, name, value));
			@SuppressWarnings("unchecked")
			var got = ((java.util.Map<String, String>) srcExpected.get(new VersionKey(key, versionId)).get(CHECKSUM))
					.get(param);
			if (!Objects.equals(got, value))
				throw new CaseFailed(key + " PUT 응답 " + param + " 가 계산값과 다름: " + got + " != " + value);
		}
		detail.add(String.join(", ", Checksums.ALGORITHMS.keySet()));
	}
}
