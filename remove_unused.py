#!/usr/bin/python3

import argparse
import json
import re
import os


def get_report_files(path):
    reports = set()
    for file_name in os.listdir(path):
        s = re.search(
            '^index_(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})\\.json$',
            file_name)
        if s:
            reports.add(file_name)
    return reports


def get_wsdls_in_report(path, report_file):
    with open(os.path.join(path, report_file), 'r') as fh:
        report_data = json.load(fh)

    used_wsdls = set()
    for system in report_data:
        for method in system['methods']:
            if method['wsdl']:
                used_wsdls.add(os.path.join(path, method['wsdl']))
    return used_wsdls


def get_available_wsdls(path):
    available_wsdls = set()
    for root, _, files in os.walk(path):
        for file_name in files:
            s = re.search('^\\d+\\.wsdl$', file_name)
            if s:
                available_wsdls.add(os.path.join(root, file_name))
    return available_wsdls


def get_unused_wsdls(path):
    reports = get_report_files(path)
    if not reports:
        print('No catalogue reports found, exiting!')
        exit(1)

    used_wsdls = set()
    for report_file in reports:
        used_wsdls = used_wsdls.union(get_wsdls_in_report(path, report_file))
    if not used_wsdls:
        print('Catalogue does not use any WSDLs? Possibly this is an error, exiting!')
        exit(1)

    available_wsdls = get_available_wsdls(path)
    return available_wsdls - used_wsdls


def main():
    parser = argparse.ArgumentParser(
        description='Remove WSDLs that are no longer used by X-tee catalogue',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('path', metavar='PATH', help='Path to the catalogue')
    parser.add_argument(
        '--only-list', action='store_true', help='Only list unused WSDLs, do not remove anything')
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print('Directory not found "{}"'.format(args.path))
        exit(1)

    unused_wsdls = get_unused_wsdls(args.path)

    if args.only_list:
        for wsdl_path in unused_wsdls:
            print(wsdl_path)
    else:
        if unused_wsdls:
            print('Removing {} unused WSDLs:'.format(len(unused_wsdls)))
            for wsdl_path in unused_wsdls:
                print(wsdl_path)
                os.remove(wsdl_path)
        else:
            print('No unused WSDLs found')


if __name__ == '__main__':
    main()
