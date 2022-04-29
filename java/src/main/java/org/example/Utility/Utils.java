package org.example.Utility;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.Base64.Encoder;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Utils {
	public static final char[] TEXT = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	public static final char[] TEXT_String = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
			'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
			'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9' };
	public static Random rand = new Random();

	public static String RandomText(int Length) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < Length; i++)
			sb.append(TEXT[rand.nextInt(TEXT.length)]);
		return sb.toString();
	}

	public static String RandomTextToLong(int Length) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < Length; i++)
			sb.append(TEXT_String[rand.nextInt(TEXT_String.length)]);
		return sb.toString();
	}

	public static ArrayList<String> GenerateRandomString(int Size, int PartSize) {
		ArrayList<String> StringList = new ArrayList<String>();

		int RemainSize = Size;
		while (RemainSize > 0) {
			int NowPartSize;
			if (RemainSize > PartSize)
				NowPartSize = PartSize;
			else
				NowPartSize = RemainSize;

			StringList.add(Utils.RandomTextToLong(NowPartSize));

			RemainSize -= NowPartSize;
		}

		return StringList;
	}

	public static String GetMD5(String str) {
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

	public static ArrayList<String> GetKeys(List<S3ObjectSummary> ObjectList) {
		if (ObjectList != null) {
			var Temp = new ArrayList<String>();

			for (var S3Object : ObjectList)
				Temp.add(S3Object.getKey());

			return Temp;
		}
		return null;
	}
}
