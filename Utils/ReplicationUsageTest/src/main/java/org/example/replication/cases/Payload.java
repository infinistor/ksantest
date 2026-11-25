package org.example.replication.cases;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.SplittableRandom;

import org.example.replication.Common;

import software.amazon.awssdk.core.sync.RequestBody;

/**
 * 업로드할 본문. 임의 본문은 메모리에 올리지 않고 시드로 같은 바이트를 몇 번이든 다시 만든다(5G 단일 PUT 대응).
 * SDK 는 서명(SHA256)과 전송에 본문을 두 번 읽으므로 다시 읽을 수 있어야 한다.
 */
public final class Payload {
	private static final SplittableRandom SEEDS = new SplittableRandom();
	private static final int BUFFER = 64 * 1024;

	private final long size;
	private final byte[] bytes; // 고정 본문이면 그 바이트, 임의 본문이면 null
	private final long seed;
	private String md5;

	private Payload(long size, byte[] bytes, long seed) {
		this.size = size;
		this.bytes = bytes;
		this.seed = seed;
	}

	public static Payload random(long size) {
		return new Payload(size, null, SEEDS.nextLong());
	}

	public static Payload of(byte[] bytes) {
		return new Payload(bytes.length, bytes, 0);
	}

	public long size() {
		return size;
	}

	public RequestBody requestBody() {
		if (bytes != null)
			return RequestBody.fromBytes(bytes);
		return RequestBody.fromContentProvider(this::stream, size, "application/octet-stream");
	}

	public synchronized String md5Hex() {
		if (md5 == null) {
			if (bytes != null) {
				md5 = Common.md5Hex(bytes);
			} else {
				try (InputStream in = stream()) {
					MessageDigest d = MessageDigest.getInstance("MD5");
					byte[] buf = new byte[BUFFER];
					int n;
					while ((n = in.read(buf)) > 0)
						d.update(buf, 0, n);
					md5 = HexFormat.of().formatHex(d.digest());
				} catch (IOException | java.security.NoSuchAlgorithmException e) {
					throw new IllegalStateException(e);
				}
			}
		}
		return md5;
	}

	private InputStream stream() {
		return new InputStream() {
			private final SplittableRandom random = new SplittableRandom(seed);
			private long left = size;
			private long word;
			private int wordLeft;

			@Override
			public int read() {
				if (left <= 0)
					return -1;
				left--;
				return nextByte();
			}

			@Override
			public int read(byte[] b, int off, int len) {
				if (left <= 0)
					return -1;
				int n = (int) Math.min(len, left);
				for (int i = 0; i < n; i++)
					b[off + i] = (byte) nextByte();
				left -= n;
				return n;
			}

			private int nextByte() {
				if (wordLeft == 0) {
					word = random.nextLong();
					wordLeft = 8;
				}
				int v = (int) (word & 0xFF);
				word >>>= 8;
				wordLeft--;
				return v;
			}
		};
	}
}
