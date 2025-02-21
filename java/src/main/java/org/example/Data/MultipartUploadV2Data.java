/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.Data;

import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class MultipartUploadV2Data {
	public String uploadId;
	public List<CompletedPart> parts;
	public StringBuilder body;
	public int partSize;

	public MultipartUploadV2Data() {
		uploadId = "";
		body = new StringBuilder();
		parts = new ArrayList<>();
		partSize = 5 * MainData.MB;
	}

	public int nextPartNumber() {
		return parts.size() + 1;
	}

	public String getBody() {
		return body.toString();
	}

	public void addPart(int partNumber, String eTag) {
		parts.add(CompletedPart.builder().partNumber(partNumber).eTag(eTag).build());
	}

	public void addPart(ChecksumAlgorithm algorithm, UploadPartResponse response) {
		switch (algorithm) {
			case CRC32:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.eTag()).checksumCRC32(response.checksumCRC32()).build());
				break;
			case CRC32_C:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.eTag()).checksumCRC32C(response.checksumCRC32C()).build());
				break;
			case CRC64_NVME:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.eTag()).checksumCRC64NVME(response.checksumCRC64NVME()).build());
				break;
			case SHA1:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.eTag()).checksumSHA1(response.checksumSHA1()).build());
				break;
			case SHA256:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.eTag()).checksumSHA256(response.checksumSHA256()).build());
				break;
			default:
				throw new IllegalArgumentException("Invalid checksum algorithm: " + algorithm);
		}
	}

	public void addPart(ChecksumAlgorithm algorithm, UploadPartCopyResponse response) {
		switch (algorithm) {
			case CRC32:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.copyPartResult().eTag()).checksumCRC32(response.copyPartResult().checksumCRC32())
						.build());
				break;
			case CRC32_C:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.copyPartResult().eTag())
						.checksumCRC32C(response.copyPartResult().checksumCRC32C()).build());
				break;
			case CRC64_NVME:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.copyPartResult().eTag())
						.checksumCRC64NVME(response.copyPartResult().checksumCRC64NVME()).build());
				break;
			case SHA1:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.copyPartResult().eTag()).checksumSHA1(response.copyPartResult().checksumSHA1())
						.build());
				break;
			case SHA256:
				parts.add(CompletedPart.builder().partNumber(nextPartNumber())
						.eTag(response.copyPartResult().eTag())
						.checksumSHA256(response.copyPartResult().checksumSHA256()).build());
				break;
			default:
				throw new IllegalArgumentException("Invalid checksum algorithm: " + algorithm);
		}
	}

	public void addPart(String eTag) {
		parts.add(CompletedPart.builder().partNumber(nextPartNumber()).eTag(eTag).build());
	}

	public void appendBody(String data) {
		body.append(data);
	}

	public CompletedMultipartUpload completedMultipartUpload() {
		return CompletedMultipartUpload.builder().parts(parts).build();
	}
}