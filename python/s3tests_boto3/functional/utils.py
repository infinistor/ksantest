#Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
#KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
#the GNU General Public License as published by the Free Software Foundation, either version 
#3 of the License.  See LICENSE for details

#본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
#KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
#KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
import random
import requests
import string
import time

from nose.tools import eq_ as eq

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass as e:
        return e
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        s = s + strpart[:(this_part_size % chunk)]
        yield s
        if (x == size):
            return

def _get_status(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    return status

def _get_status_and_error_code(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    error_code = response['Error']['Code']
    return status, error_code
