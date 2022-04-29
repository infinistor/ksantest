package org.example.auth;

import static org.junit.jupiter.api.Assertions.fail;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class AWS2SignerBase {
	
	public static String GetBase64EncodedSHA1Hash(String Policy, String SecretKey) {
		var signingKey = new SecretKeySpec(SecretKey.getBytes(), "HmacSHA1");
		Mac mac;
		try {
			mac = Mac.getInstance("HmacSHA1");
			mac.init(signingKey);
		} catch (NoSuchAlgorithmException e) {
			fail(e.getMessage());
			return "";
		} catch (InvalidKeyException e) {
			fail(e.getMessage());
			return "";
		}

		var encoder = Base64.getEncoder();
		return encoder.encodeToString((mac.doFinal(Policy.getBytes())));
	}
}
