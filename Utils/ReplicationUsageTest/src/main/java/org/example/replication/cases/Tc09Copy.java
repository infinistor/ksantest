package org.example.replication.cases;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.example.replication.CaseFailed;
import org.example.replication.Common;

import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.TaggingDirective;

/** TC09 CopyObject: 같은 버킷 안 복사와 다른 버킷에서 가져온 복사가 모두 복제된다. */
public class Tc09Copy extends ReplicationCase {
	static final String COPY_SUFFIX = "-copy";
	static final String AUX_PREFIX = "from-aux/";

	public Tc09Copy() {
		super("TC09", "CopyObject 복제", "같은 버킷·다른 버킷 Copy");
	}

	void copy(String sourceBucket, String sourceKey, String key, Map<String, Object> sourceInfo) {
		var r = s3.copyObject(b -> b.sourceBucket(sourceBucket).sourceKey(sourceKey)
				.destinationBucket(src).destinationKey(key)
				.metadataDirective(MetadataDirective.COPY).taggingDirective(TaggingDirective.COPY));
		String etag = Common.stripQuotes(r.copyObjectResult().eTag());
		if (!Objects.equals(etag, sourceInfo.get(ETAG)))
			throw new CaseFailed(key + " Copy ETag 가 원본과 다름: " + etag + " != " + sourceInfo.get(ETAG));
		record(key, r.versionId(), new LinkedHashMap<>(sourceInfo), null);
	}

	@Override
	protected void operate(List<String> detail) {
		String aux = newExtraBucket("aux"); // 복제 룰이 없는 원천 버킷
		var keys = sizedKeys();
		for (var k : keys) {
			String versionId = put(k.key(), k.size());
			copy(src, k.key(), k.key() + COPY_SUFFIX, srcExpected.get(new VersionKey(k.key(), versionId)));
		}
		for (var k : keys) {
			Payload body = Payload.random(k.size());
			Map<String, String> meta = Map.of(META_KEY, k.key());
			Map<String, String> tags = sourceTags(k.key(), null);
			s3.putObject(b -> b.bucket(aux).key(k.key()).metadata(meta).tagging(taggingHeader(tags)), body.requestBody());
			copy(aux, k.key(), AUX_PREFIX + k.key(), info(k.size(), body.md5Hex(), meta, tags));
		}
		detail.add("PUT " + keys.size() + "개, 같은 버킷 Copy " + keys.size() + "개, " + aux + " 에서 Copy "
				+ keys.size() + "개");
	}
}
