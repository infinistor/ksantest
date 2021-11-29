/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version 
* 3 of the License.  See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.s3tests;

import java.nio.ByteBuffer;
import java.security.AlgorithmParameters;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class AES256 {
	public static String encryptAES256(String msg, String key) throws Exception {
  
	    SecureRandom random = new SecureRandom();
	    byte bytes[] = new byte[20];
	    random.nextBytes(bytes);
	    byte[] saltBytes = bytes;

	    // Password-Based Key Derivation function 2
	    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");

	    PBEKeySpec spec = new PBEKeySpec(key.toCharArray(), saltBytes, 70000, 256);

	    SecretKey secretKey = factory.generateSecret(spec);
	    SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");

	    // CBC : Cipher Block Chaining Mode
	    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
	    cipher.init(Cipher.ENCRYPT_MODE, secret);
	    
	    AlgorithmParameters params = cipher.getParameters();

	    byte[] ivBytes = params.getParameterSpec(IvParameterSpec.class).getIV();
	    byte[] encryptedTextBytes = cipher.doFinal(msg.getBytes("UTF-8"));
	    byte[] buffer = new byte[saltBytes.length + ivBytes.length + encryptedTextBytes.length];
	    System.arraycopy(saltBytes, 0, buffer, 0, saltBytes.length);
	    System.arraycopy(ivBytes, 0, buffer, saltBytes.length, ivBytes.length);
	    System.arraycopy(encryptedTextBytes, 0, buffer, saltBytes.length + ivBytes.length, encryptedTextBytes.length);

	    return Base64.getEncoder().encodeToString(buffer);
	}
	
	public static String decryptAES256(String msg, String key) throws Exception {

	    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
	    ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(msg));

	    byte[] saltBytes = new byte[20];
	    buffer.get(saltBytes, 0, saltBytes.length);
	    byte[] ivBytes = new byte[cipher.getBlockSize()];
	    buffer.get(ivBytes, 0, ivBytes.length);
	    byte[] encryoptedTextBytes = new byte[buffer.capacity() - saltBytes.length - ivBytes.length];
	    buffer.get(encryoptedTextBytes);

	    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
	    PBEKeySpec spec = new PBEKeySpec(key.toCharArray(), saltBytes, 70000, 256);

	    SecretKey secretKey = factory.generateSecret(spec);
	    SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");

	    cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(ivBytes));

	    byte[] decryptedTextBytes = cipher.doFinal(encryoptedTextBytes);
	    return new String(decryptedTextBytes);
	}
}