#!/usr/bin/python3

import argparse
import json
import re
import os


def sort_by_time(item):
    return item['reportTime']


def get_catalogue_reports(path):
    reports = []
    for file_name in os.listdir(path):
        s = re.search(
            '^index_(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})\\.json$',
            file_name)
        if s:
            reports.append({
                'reportTime': '{}-{}-{} {}:{}:{}'.format(
                    s.group(1), s.group(2), s.group(3), s.group(4), s.group(5), s.group(6)),
                'reportPath': file_name})
    reports.sort(key=sort_by_time, reverse=True)
    return reports


def main():
    parser = argparse.ArgumentParser(
        description='Recreate history.json file for X-tee catalogue',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('path', metavar='PATH', help='Path to the catalogue')
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(u'Directory not found "{}"'.format(args.path))
        exit(1)

    reports = get_catalogue_reports(args.path)
    if len(reports):
        with open(u'{}/history.json'.format(args.path), 'w') as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
        print('Writing {} reports to {}/history.json'.format(len(reports), args.path))
    else:
        print('No JSON reports found in directory: {}'.format(args.path))


if __name__ == '__main__':
    main()
