package org.example.Utility;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.Base64.Encoder;
import java.util.Collections;

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

	public static String getNewBucketName(String prefix) {
		String bucketName = prefix + Utils.randomText(BUCKET_MAX_LENGTH);
		return bucketName.substring(0, BUCKET_MAX_LENGTH - 1);
	}

	public static String makeArnResource(String path) {
		return String.format("arn:aws:s3:::%s", path);
	}

}
