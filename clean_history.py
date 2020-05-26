#!/usr/bin/python3

import argparse
import re
import os
from datetime import datetime, timedelta


def sort_by_time(item):
    return item['time']


def get_catalogue_reports(path):
    reports = []
    for file_name in os.listdir(path):
        s = re.search(
            '^index_(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})\\.json$',
            file_name)
        if s:
            reports.append({
                'time': datetime(
                    int(s.group(1)), int(s.group(2)), int(s.group(3)),
                    int(s.group(4)), int(s.group(5)), int(s.group(6))),
                'path': file_name})
    reports.sort(key=sort_by_time, reverse=True)
    return reports


def get_paths_to_keep(reports, fresh_time):
    """Get paths that must not be removed"""
    # Latest report is never deleted
    unique_paths = {reports[0]['time']: reports[0]['path']}

    filtered_items = {}
    for report in reports:
        if report['time'] >= fresh_time:
            # Keeping all fresh reports
            unique_paths[report['time']] = report['path']
        else:
            # Searching for the first report in a day
            item_key = datetime(report['time'].year, report['time'].month, report['time'].day)
            if item_key not in filtered_items or report['time'] < filtered_items[item_key]['time']:
                filtered_items[item_key] = {'time': report['time'], 'path': report['path']}

    # Adding first report of the day
    for item in filtered_items.values():
        unique_paths[item['time']] = item['path']

    paths_to_keep = list(unique_paths.values())
    paths_to_keep.sort()

    return paths_to_keep


def get_old_reports(path, days_keep):
    old_reports = []
    all_reports = get_catalogue_reports(path)
    cur_time = datetime.today()
    fresh_time = datetime(cur_time.year, cur_time.month, cur_time.day) - timedelta(days=days_keep)
    paths_to_keep = get_paths_to_keep(all_reports, fresh_time)

    for report in all_reports:
        if not report['path'] in paths_to_keep:
            old_reports.append(report['path'])

    old_reports.sort()
    return old_reports


def main():
    parser = argparse.ArgumentParser(
        description='Remove older versions of X-tee catalogue to free up disk space',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('path', metavar='PATH', help='Path to the catalogue')
    parser.add_argument(
        'days_to_keep', metavar='DAYS_TO_KEEP',
        help='Amount of last days to keep without cleaning', type=int)
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print('Directory not found "{}"'.format(args.path))
        exit(1)

    old_reports = get_old_reports(args.path, args.days_to_keep)
    if len(old_reports):
        print('Removing {} old JSON reports:'.format(len(old_reports)))
        for report_path in old_reports:
            print('{}/{}'.format(args.path, report_path))
            os.remove('{}/{}'.format(args.path, report_path))
    else:
        print('No old JSON reports found in directory: {}'.format(args.path))


if __name__ == '__main__':
    main()
