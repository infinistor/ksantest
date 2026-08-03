package s3tests

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// [AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
func TestCseEncryptedTransfer1b(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 1)
	key := deterministicBody(32)
	plain := deterministicBody(1)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseEncryptedTransfer1b", encoded, map[string]string{"x-amz-meta-key": string(key)})
	got := getObjectBytes(t, s.client, bucket, "testCseEncryptedTransfer1b")
	decrypted, err := cseDecrypt(got, key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("CSE round trip size=%d err=%v", len(decrypted), err)
	}
}

// [AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
func TestCseEncryptedTransfer1kb(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 2)
	key := deterministicBody(32)
	plain := deterministicBody(1024)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseEncryptedTransfer1kb", encoded, map[string]string{"x-amz-meta-key": string(key)})
	got := getObjectBytes(t, s.client, bucket, "testCseEncryptedTransfer1kb")
	decrypted, err := cseDecrypt(got, key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("CSE round trip size=%d err=%v", len(decrypted), err)
	}
}

// [AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
func TestCseEncryptedTransfer1MB(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 3)
	key := deterministicBody(32)
	plain := deterministicBody(1024 * 1024)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseEncryptedTransfer1MB", encoded, map[string]string{"x-amz-meta-key": string(key)})
	got := getObjectBytes(t, s.client, bucket, "testCseEncryptedTransfer1MB")
	decrypted, err := cseDecrypt(got, key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("CSE round trip size=%d err=%v", len(decrypted), err)
	}
}

// [AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
func TestCseEncryptedTransfer13b(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 4)
	key := deterministicBody(32)
	plain := deterministicBody(13)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseEncryptedTransfer13b", encoded, map[string]string{"x-amz-meta-key": string(key)})
	got := getObjectBytes(t, s.client, bucket, "testCseEncryptedTransfer13b")
	decrypted, err := cseDecrypt(got, key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("CSE round trip size=%d err=%v", len(decrypted), err)
	}
}

// [AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인
func TestCseEncryptionMethodHead(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 5)
	key := deterministicBody(32)
	encoded := cseEncrypt(t, deterministicBody(1000), key)
	metadata := map[string]string{"key": string(key)}
	putCSEBytes(t, s.client, bucket, "testCseEncryptionMethodHead/obj", encoded, metadata)
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("testCseEncryptionMethodHead/obj")})
	if err != nil || head.Metadata["key"] != metadata["key"] {
		t.Fatalf("HeadObject metadata=%v err=%v", head.Metadata, err)
	}
}

// [AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치
func TestCseEncryptionNonDecryption(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 6)
	plain, key := deterministicBody(1000), deterministicBody(32)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseEncryptionNonDecryption/obj", encoded, nil)
	if got := getObjectBytes(t, s.client, bucket, "testCseEncryptionNonDecryption/obj"); bytes.Equal(got, plain) {
		t.Fatal("raw encrypted object unexpectedly equals plaintext")
	}
}

// [AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인
func TestCseNonEncryptionDecryption(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 7)
	plain, key := deterministicBody(1000), deterministicBody(32)
	putCSEBytes(t, s.client, bucket, "testCseNonEncryptionDecryption", plain, nil)
	stored := getObjectBytes(t, s.client, bucket, "testCseNonEncryptionDecryption")
	if _, err := cseDecrypt(stored, key); err == nil {
		t.Fatal("decrypting plaintext unexpectedly succeeded")
	}
}

// [AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공
func TestCseEncryptionRangeRead(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 8)
	encoded := cseEncrypt(t, deterministicBody(1024*1024), deterministicBody(32))
	putCSEBytes(t, s.client, bucket, "testCseEncryptionRangeRead", encoded, map[string]string{"x-amz-meta-key": string(deterministicBody(32))})
	start := 397
	assertRange(t, s.client, bucket, "testCseEncryptionRangeRead", encoded, start, start+999)
}

// [AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인
func TestCseEncryptionMultipartUpload(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 9)
	key := deterministicBody(32)
	encoded := cseEncrypt(t, deterministicBody(50*1024*1024), key)
	metadata := map[string]string{"key": string(key)}
	completeMultipart(t, s.client, bucket, "testCseEncryptionMultipartUpload", encoded, false, metadata)
	listed, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil || aws.ToInt32(listed.KeyCount) != 1 || len(listed.Contents) != 1 || aws.ToInt64(listed.Contents[0].Size) != int64(len(encoded)) {
		t.Fatalf("ListObjectsV2=%#v err=%v", listed, err)
	}
	head, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("testCseEncryptionMultipartUpload")})
	if err != nil || head.Metadata["key"] != string(key) || aws.ToString(head.ContentType) != "text/plain" {
		t.Fatalf("HeadObject metadata=%v type=%q err=%v", head.Metadata, aws.ToString(head.ContentType), err)
	}
	assertObjectRanges(t, s.client, bucket, "testCseEncryptionMultipartUpload", encoded, []int{1024 * 1024, 10 * 1024 * 1024})
	for index := 0; index < 100; index++ {
		length := 1 + (index*7919)%65536
		start := (index * 104729) % (len(encoded) - length)
		assertRange(t, s.client, bucket, "testCseEncryptionMultipartUpload", encoded, start, start+length-1)
	}
}

// CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
func TestCseGetObjectMany(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 10)
	plain, key := deterministicBody(15*1024*1024), deterministicBody(32)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseGetObjectMany", encoded, nil)
	decrypted, err := cseDecrypt(getObjectBytes(t, s.client, bucket, "testCseGetObjectMany"), key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("decrypt err=%v", err)
	}
	for index := 0; index < 50; index++ {
		if got := getObjectBytes(t, s.client, bucket, "testCseGetObjectMany"); !bytes.Equal(got, encoded) {
			t.Fatalf("GET %d encrypted body mismatch", index)
		}
	}
}

// CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
func TestCseRangeObjectMany(t *testing.T) {
	t.Parallel()
	s := newSuite(t)
	bucket := s.bucket(t, 11)
	plain, key := deterministicBody(15*1024*1024), deterministicBody(32)
	encoded := cseEncrypt(t, plain, key)
	putCSEBytes(t, s.client, bucket, "testCseRangeObjectMany", encoded, nil)
	decrypted, err := cseDecrypt(getObjectBytes(t, s.client, bucket, "testCseRangeObjectMany"), key)
	if err != nil || !bytes.Equal(decrypted, plain) {
		t.Fatalf("decrypt err=%v", err)
	}
	for index := 0; index < 50; index++ {
		length := 1 + (index*7919)%65536
		start := (index * 104729) % (len(encoded) - length)
		assertRange(t, s.client, bucket, "testCseRangeObjectMany", encoded, start, start+length-1)
	}
}

func putCSEBytes(t *testing.T, client *s3.Client, bucket, key string, body []byte, metadata map[string]string) {
	t.Helper()
	length := int64(len(body))
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(length), ContentType: aws.String("text/plain"), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
}

func getObjectBytes(t *testing.T, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func cseEncrypt(t *testing.T, plaintext, password []byte) []byte {
	t.Helper()
	salt, iv := make([]byte, 20), make([]byte, aes.BlockSize)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	key := pbkdf2SHA1(password, salt, 70000, 32)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append(append([]byte(nil), plaintext...), bytes.Repeat([]byte{byte(padding)}, padding)...)
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	packed := append(append(append([]byte(nil), salt...), iv...), ciphertext...)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(packed)))
	base64.StdEncoding.Encode(encoded, packed)
	return encoded
}

func cseDecrypt(encoded, password []byte) ([]byte, error) {
	packed := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(packed, encoded)
	if err != nil {
		return nil, err
	}
	packed = packed[:n]
	if len(packed) < 20+aes.BlockSize+aes.BlockSize || (len(packed)-20-aes.BlockSize)%aes.BlockSize != 0 {
		return nil, errors.New("invalid CSE payload length")
	}
	salt, iv, ciphertext := packed[:20], packed[20:36], packed[36:]
	block, err := aes.NewCipher(pbkdf2SHA1(password, salt, 70000, 32))
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, ciphertext)
	padding := int(plaintext[len(plaintext)-1])
	if padding < 1 || padding > aes.BlockSize || padding > len(plaintext) {
		return nil, errors.New("invalid CSE padding")
	}
	for _, value := range plaintext[len(plaintext)-padding:] {
		if int(value) != padding {
			return nil, errors.New("invalid CSE padding")
		}
	}
	return plaintext[:len(plaintext)-padding], nil
}

func pbkdf2SHA1(password, salt []byte, iterations, length int) []byte {
	result := make([]byte, 0, length)
	for block := uint32(1); len(result) < length; block++ {
		counter := []byte{byte(block >> 24), byte(block >> 16), byte(block >> 8), byte(block)}
		mac := hmac.New(sha1.New, password)
		_, _ = mac.Write(salt)
		_, _ = mac.Write(counter)
		u := mac.Sum(nil)
		t := append([]byte(nil), u...)
		for iteration := 1; iteration < iterations; iteration++ {
			mac = hmac.New(sha1.New, password)
			_, _ = mac.Write(u)
			u = mac.Sum(nil)
			for index := range t {
				t[index] ^= u[index]
			}
		}
		result = append(result, t...)
	}
	return result[:length]
}
