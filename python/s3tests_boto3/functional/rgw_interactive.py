#Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
#KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
#the GNU General Public License as published by the Free Software Foundation, either version 
#3 of the License.  See LICENSE for details

#본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
#KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
#KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
#!/usr/bin/python
import boto3
import os
import random
import string
import itertools

host = "127.0.0.1"
port = 8080

## AWS access key
access_key = "123456789"

## AWS secret key
secret_key = "123456789"

prefix = "my-test-"

endpoint_url = "http://%s:%d" % (host, port)

client = boto3.client(service_name='s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url,
                    use_ssl=False,
                    verify=False)

s3 = boto3.resource('s3', 
                    use_ssl=False,
                    verify=False,
                    endpoint_url=endpoint_url, 
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

bucket_counter = itertools.count(1)

def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name

def get_new_bucket(session=boto3, name=None, headers=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    s3 = session.resource('s3', 
                        use_ssl=False,
                        verify=False,
                        endpoint_url=endpoint_url, 
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    if name is None:
        name = get_new_bucket_name()
    bucket = s3.Bucket(name)
    bucket_location = bucket.create()
    return bucket
