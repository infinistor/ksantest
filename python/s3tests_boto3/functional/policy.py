#Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
#KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
#the GNU General Public License as published by the Free Software Foundation, either version 
#3 of the License.  See LICENSE for details

#본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
#KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
#KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
import json

class Statement(object):
    def __init__(self, action, resource, principal = {"AWS" : "*"}, effect= "Allow", condition = None):
        self.principal = principal
        self.action = action
        self.resource = resource
        self.condition = condition
        self.effect = effect

    def to_dict(self):
        d = { "Action" : self.action,
              "Principal" : self.principal,
              "Effect" : self.effect,
              "Resource" : self.resource
        }

        if self.condition is not None:
            d["Condition"] = self.condition

        return d

class Policy(object):
    def __init__(self):
        self.statements = []

    def add_statement(self, s):
        self.statements.append(s)
        return self

    def to_json(self):
        policy_dict = {
            "Version" : "2012-10-17",
            "Statement":
            [s.to_dict() for s in self.statements]
        }

        return json.dumps(policy_dict)

def make_json_policy(action, resource, principal={"AWS": "*"}, conditions=None):
    """
    Helper function to make single statement policies
    """
    s = Statement(action, resource, principal, condition=conditions)
    p = Policy()
    return p.add_statement(s).to_json()
