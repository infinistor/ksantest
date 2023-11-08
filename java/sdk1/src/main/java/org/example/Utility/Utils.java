package org.example.Utility;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.Base64.Encoder;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Utils {
	public static final char[] TEXT = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	public static final char[] TEXT_LONG = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
			'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
			'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9' };
	public static Random rand = new Random();

	public static String randomText(int length) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < length; i++)
			sb.append(TEXT[rand.nextInt(TEXT.length)]);
		return sb.toString();
	}

	public static String randomTextToLong(int length) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < length; i++)
			sb.append(TEXT_LONG[rand.nextInt(TEXT_LONG.length)]);
		return sb.toString();
	}

	public static ArrayList<String> generateRandomString(int size, int partSize) {
		ArrayList<String> stringList = new ArrayList<String>();

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
			md.update(str.getBytes("UTF-8"));

			byte byteData[] = md.digest();

			Encoder encoder = Base64.getEncoder();
			var encodeBytes = encoder.encode(byteData);
			return new String(encodeBytes);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ArrayList<String> getKeys(List<S3ObjectSummary> ObjectList) {
		if (ObjectList != null) {
			var Temp = new ArrayList<String>();

			for (var S3Object : ObjectList)
				Temp.add(S3Object.getKey());

			return Temp;
		}
		return null;
	}

	public static boolean createDummyFile(String filePath, int size) {
		try {
			var file = new File(filePath);
			file.createNewFile();
			var fos = new FileOutputStream(file);
			var buffer = new byte[1048576];
			for (long i = 0; i < size; i+=buffer.length) {
				fos.write(buffer, 0, buffer.length);
			}
			fos.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
