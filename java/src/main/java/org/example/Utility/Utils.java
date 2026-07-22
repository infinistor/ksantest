package org.example.Utility;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Utils {
	private Utils() {
	}

	private static Random rand = new Random(System.currentTimeMillis());

	protected static final char[] TEXT = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
			'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	protected static final char[] TEXT_LONG = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
			'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
			'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9' };
	static final int BUCKET_MAX_LENGTH = 63;

	public static String randomText(int length) {
		var sb = new StringBuilder();

		for (int i = 0; i < length; i++)
			sb.append(TEXT[rand.nextInt(TEXT.length)]);
		return sb.toString();
	}

	public static String randomTextToLong(int length) {
		var sb = new StringBuilder();

		for (int i = 0; i < length; i++)
			sb.append(TEXT_LONG[rand.nextInt(TEXT_LONG.length)]);
		return sb.toString();
	}

	public static List<String> generateRandomString(int size, int partSize) {
		var stringList = new ArrayList<String>();

		int remainSize = size;
		while (remainSize > 0) {
			int nowPartSize;
			if (remainSize > partSize)
				nowPartSize = partSize;
			else
				nowPartSize = remainSize;

			stringList.add(Utils.randomTextToLong(nowPartSize));

			remainSize -= nowPartSize;
		}

		return stringList;
	}

	public static String randomObjectName(int length) {
		// 200자에 /를 추가하여 생성하도록 구현
		final int MAX_LENGTH = 200;

		var name = new StringBuilder();
		int index = 0;
		while (name.length() <= length) {
			if (index + MAX_LENGTH < length) {
				name.append(randomTextToLong(MAX_LENGTH));
				index += MAX_LENGTH;
			} else {
				name.append(randomTextToLong(length - index));
				index = length;
			}
			name.append('/');
		}
		return name.toString().substring(0, length);

	}

	public static String getMD5(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes(StandardCharsets.UTF_8));

			byte[] byteData = md.digest();

			Encoder encoder = Base64.getEncoder();
			var encodeBytes = encoder.encode(byteData);
			return new String(encodeBytes);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<String> getKeys(List<S3ObjectSummary> objectList) {
		if (objectList != null) {
			var temp = new ArrayList<String>();

			for (var S3Object : objectList)
				temp.add(S3Object.getKey());

			return temp;
		}
		return Collections.emptyList();
	}

	// create dummy file
	public static void createDummyFile(String filePath, int size) {
		byte[] bytes = new byte[size];
		rand.nextBytes(bytes);

		try (FileOutputStream fos = new FileOutputStream(filePath)) {
			fos.write(bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getUTCTime2String(int expireTime) {
		var now = Instant.now();
		var secondsBefore = now.minus(expireTime, ChronoUnit.SECONDS);
		var formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
		return formatter.format(secondsBefore);
	}

	/**
	 * 버킷명 길이/형식 검증용. suite/testId 없이 prefix+랜덤만 사용한다.
	 */
	public static String randomBucketName(String prefix) {
		if (prefix == null)
			prefix = "";
		final int maxLen = BUCKET_MAX_LENGTH - 1;
		String bucketName = prefix + randomText(BUCKET_MAX_LENGTH);
		return bucketName.substring(0, Math.min(maxLen, bucketName.length()));
	}

	/**
	 * 추적 가능한 버킷명: {@code {prefix}{suite}-{testId}-{random}}
	 * <p>
	 * 예: {@code v2-java-putobject-3-k3m9x2ab...}<br>
	 * {@code testId}는 테스트 코드에 명시한 번호이며, 추가/순서 변경에도 기존 번호는 유지한다.
	 */
	public static String getNewBucketName(String prefix, String suite, int testId) {
		if (prefix == null)
			prefix = "";
		if (suite == null || suite.isEmpty())
			suite = "x";
		suite = suite.toLowerCase().replaceAll("[^a-z0-9]", "");
		if (suite.isEmpty())
			suite = "x";

		final int maxLen = BUCKET_MAX_LENGTH - 1;
		final int minRandom = 6;
		String idxPart = Integer.toString(testId);

		int reserved = prefix.length() + 1 + idxPart.length() + 1 + minRandom;
		int suiteMax = maxLen - reserved;
		if (suiteMax < 1)
			return randomBucketName(prefix);
		if (suite.length() > suiteMax)
			suite = suite.substring(0, suiteMax);

		String head = prefix + suite + "-" + idxPart + "-";
		int randomLen = maxLen - head.length();
		String bucketName = head + randomText(Math.max(randomLen, 0));
		if (bucketName.length() > maxLen)
			bucketName = bucketName.substring(0, maxLen);
		return bucketName;
	}

	/** 클래스 FQCN 또는 simple name → 버킷용 suite 약어. */
	public static String toSuiteId(String className) {
		if (className == null || className.isEmpty())
			return "x";
		int dot = className.lastIndexOf('.');
		String simple = (dot >= 0 ? className.substring(dot + 1) : className).toLowerCase()
				.replaceAll("[^a-z0-9]", "");
		return simple.isEmpty() ? "x" : simple;
	}

	public static String makeArnResource(String path) {
		return String.format("arn:aws:s3:::%s", path);
	}

}
