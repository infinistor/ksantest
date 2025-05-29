#!/usr/bin/env python
import sys
import glob
import xml.etree.ElementTree as ET
from typing import List

"""Merge multiple JUnit XML files into a single results file.
Output dumps to stdout.
example usage:
    $ python merge_junit_results.py results1.xml results2.xml > results.xml
"""


def main():
    args = sys.argv[1:]
    if not args:
        usage()
        sys.exit(2)
    if "-h" in args or "--help" in args:
        usage()
        sys.exit(2)

    # Expand wildcards
    expanded_args = []
    for arg in args:
        expanded_args.extend(glob.glob(arg))

    merge_results(expanded_args)


def merge_results(xml_files: List[str]) -> None:
    tests = 0
    errors = 0
    skipped = 0
    failures = 0
    time = 0.0
    cases = []

    for file_name in xml_files:
        try:
            tree = ET.parse(file_name)
            test_suite = tree.getroot()
            
            # 필수 속성이 없을 경우 기본값 0 사용
            tests += int(test_suite.attrib.get("tests", 0))
            errors += int(test_suite.attrib.get("errors", 0))
            skipped += int(test_suite.attrib.get("skipped", 0))
            failures += int(test_suite.attrib.get("failures", 0))
            time += float(test_suite.attrib.get("time", 0.0))
            
            # properties 요소 제거
            for case in test_suite:
                for prop in case.findall("properties"):
                    case.remove(prop)
            cases.extend(list(test_suite))
        except ET.ParseError:
            print(f"Warning: Failed to parse {file_name}", file=sys.stderr)
            continue

    new_root = ET.Element("testsuite")
    new_root.attrib["tests"] = str(tests)
    new_root.attrib["errors"] = str(errors)
    new_root.attrib["skipped"] = str(skipped)
    new_root.attrib["failures"] = str(failures)
    new_root.attrib["time"] = str(time)
    
    for case in cases:
        new_root.append(case)

    tree = ET.ElementTree(new_root)
    
    # UTF-8로 인코딩하여 출력
    xml_content = ET.tostring(new_root, encoding='utf-8', xml_declaration=True)
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout.buffer.write(xml_content)
    else:
        sys.stdout.write(xml_content.decode('utf-8'))


def usage():
    print("Usage: python merge_junit_results.py <xml_file1> <xml_file2> ...")


if __name__ == "__main__":
    main()
