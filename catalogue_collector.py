#!/usr/bin/env python3

"""This is a module for collection of X-Road services information."""

import queue
from threading import Thread, Event, Lock
import argparse
import hashlib
import json
import logging.config
import os
import re
import shutil
import time
import xrdinfo

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0

# Do not use threading by default
DEFAULT_THREAD_COUNT = 1

DEFAULT_WSDL_REPLACES = [
    # [Pattern, Replacement]
    # Example:
    # "Genereerimise aeg: 22.03.2019 08:00:30"
    # [
    #     'Genereerimise aeg: \\d{2}\\.\\d{2}\\.\\d{4} \\d{2}:\\d{2}:\\d{2}',
    #     'Genereerimise aeg: DELETED'
    # ]
]

# This logger will be used before loading of logger configuration
DEFAULT_LOGGER = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(threadName)s - %(levelname)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'WARNING',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stderr',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': True
        },
        'catalogue-collector': {
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': False
        },
    }
}

# Application will use this logger
LOGGER = logging.getLogger('catalogue-collector')


def load_config(config_file):
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as conf:
            return json.load(conf)
    except IOError as err:
        LOGGER.error('Cannot load configuration file "%s": %s', config_file, str(err))
        return None
    except json.JSONDecodeError as err:
        LOGGER.error('Invalid JSON configuration file "%s": %s', config_file, str(err))
        return None


def configure_logging(config):
    """Configure logging based on loaded configuration"""
    if 'logging-config' in config:
        logging.config.dictConfig(config['logging-config'])
        LOGGER.info('Logger configured')


def set_params(config):
    """Configure parameters based on loaded configuration"""
    params = {
        'path': None,
        'url': None,
        'client': None,
        'instance': None,
        'timeout': DEFAULT_TIMEOUT,
        'verify': False,
        'cert': None,
        'thread_cnt': DEFAULT_THREAD_COUNT,
        'wsdl_replaces': DEFAULT_WSDL_REPLACES,
        'excluded_member_codes': [],
        'excluded_subsystem_codes': [],
        'work_queue': queue.Queue(),
        'results': {},
        'results_lock': Lock(),
        'shutdown': Event()
    }

    if 'output_path' in config:
        params['path'] = config['output_path']
        LOGGER.info('Configuring "path": %s', params['path'])
    else:
        LOGGER.error('Configuration error: Output path is not provided')
        return None

    if 'server_url' in config:
        params['url'] = config['server_url']
        LOGGER.info('Configuring "url": %s', params['url'])
    else:
        LOGGER.error('Configuration error: Local Security Server URL is not provided')
        return None

    if 'client' in config and len(config['client']) in (3, 4):
        params['client'] = config['client']
        LOGGER.info('Configuring "client": %s', params['client'])
    else:
        LOGGER.error(
            'Configuration error: Client identifier is incorrect. Expecting list of identifiers. '
            'Example: ["INST", "CLASS", "MEMBER_CODE", "MEMBER_CLASS"])')
        return None

    if 'instance' in config and config['instance']:
        params['instance'] = config['instance']
        LOGGER.info('Configuring "instance": %s', params['instance'])

    if 'timeout' in config and config['timeout'] > 0.0:
        params['timeout'] = config['timeout']
        LOGGER.info('Configuring "timeout": %s', params['timeout'])

    if 'server_cert' in config and config['server_cert']:
        params['verify'] = config['server_cert']
        LOGGER.info('Configuring "verify": %s', params['verify'])

    if 'client_cert' in config and 'client_key' in config \
            and config['client_cert'] and config['client_key']:
        params['cert'] = (config['client_cert'], config['client_key'])
        LOGGER.info('Configuring "cert": %s', params['cert'])

    if 'thread_count' in config and config['thread_count'] > 0:
        params['thread_cnt'] = config['thread_count']
        LOGGER.info('Configuring "thread_cnt": %s', params['thread_cnt'])

    if 'wsdl_replaces' in config:
        params['wsdl_replaces'] = config['wsdl_replaces']
        LOGGER.info('Configuring "wsdl_replaces": %s', params['wsdl_replaces'])

    if 'excluded_member_codes' in config:
        params['excluded_member_codes'] = config['excluded_member_codes']
        LOGGER.info('Configuring "excluded_member_codes": %s', params['excluded_member_codes'])

    if 'excluded_subsystem_codes' in config:
        params['excluded_subsystem_codes'] = config['excluded_subsystem_codes']
        LOGGER.info(
            'Configuring "excluded_subsystem_codes": %s', params['excluded_subsystem_codes'])

    LOGGER.info('Configuration done')

    return params


def make_dirs(path):
    """Create directories if they do not exist"""
    try:
        os.makedirs(path)
    except OSError:
        pass
    if not os.path.exists(path):
        LOGGER.error('Cannot create directory "%s"', path)
        return False
    return True


def hash_wsdls(path):
    """Find hashes of all WSDL's in directory"""
    hashes = {}
    for file_name in os.listdir(path):
        search_res = re.search('^(\\d+)\\.wsdl$', file_name)
        if search_res:
            # Reading as bytes to avoid line ending conversion
            with open('{}/{}'.format(path, file_name), 'rb') as wsdl_file:
                wsdl = wsdl_file.read()
            hashes[file_name] = hashlib.md5(wsdl).hexdigest()
    return hashes


def save_wsdl(path, hashes, wsdl, wsdl_replaces):
    """Save WSDL if it does not exist yet"""
    # Replacing dynamically generated comments in WSDL to avoid new WSDL
    # creation because of comments.
    for wsdl_replace in wsdl_replaces:
        wsdl = re.sub(wsdl_replace[0], wsdl_replace[1], wsdl)
    wsdl_hash = hashlib.md5(wsdl.encode('utf-8')).hexdigest()
    max_wsdl = -1
    for file_name in hashes.keys():
        if wsdl_hash == hashes[file_name]:
            # Matching WSDL found
            return file_name, hashes
        search_res = re.search('^(\\d+)\\.wsdl$', file_name)
        if search_res:
            if int(search_res.group(1)) > max_wsdl:
                max_wsdl = int(search_res.group(1))
    # Creating new file
    new_file = '{}.wsdl'.format(int(max_wsdl) + 1)
    # Writing as bytes to avoid line ending conversion
    with open('{}/{}'.format(path, new_file), 'wb') as wsdl_file:
        wsdl_file.write(wsdl.encode('utf-8'))
    hashes[new_file] = wsdl_hash
    return new_file, hashes


def worker(params):
    """Main function for worker threads"""
    while True:
        # Checking periodically if it is the time to gracefully shutdown
        # the worker.
        try:
            subsystem = params['work_queue'].get(True, 0.1)
            LOGGER.info('Start processing %s', xrdinfo.stringify(subsystem))
        except queue.Empty:
            if params['shutdown'].is_set():
                return
            continue
        wsdl_rel_path = ''
        try:
            wsdl_rel_path = xrdinfo.stringify(subsystem)
            wsdl_path = '{}/{}'.format(params['path'], wsdl_rel_path)
            make_dirs(wsdl_path)
            hashes = hash_wsdls(wsdl_path)

            method_index = {}
            skip_methods = False
            for method in sorted(xrdinfo.methods(
                    addr=params['url'], client=params['client'], producer=subsystem,
                    method='listMethods', timeout=params['timeout'], verify=params['verify'],
                    cert=params['cert'])):
                if xrdinfo.stringify(method) in method_index:
                    # Method already found in previous WSDL's
                    continue

                if skip_methods:
                    # Skipping, because previous getWsdl request timed
                    # out
                    LOGGER.info('%s - SKIPPING', xrdinfo.stringify(method))
                    method_index[xrdinfo.stringify(method)] = 'SKIPPED'
                    continue

                try:
                    wsdl = xrdinfo.wsdl(
                        addr=params['url'], client=params['client'], service=method,
                        timeout=params['timeout'], verify=params['verify'], cert=params['cert'])
                except xrdinfo.RequestTimeoutError:
                    # Skipping all following requests to that subsystem
                    skip_methods = True
                    LOGGER.info('%s - TIMEOUT', xrdinfo.stringify(method))
                    method_index[xrdinfo.stringify(method)] = 'TIMEOUT'
                    continue
                except xrdinfo.XrdInfoError as err:
                    if str(err) == 'SoapFault: Service is a REST service and does not have a WSDL':
                        # We do not want to spam messages about REST services
                        LOGGER.debug('%s: %s', xrdinfo.stringify(method), err)
                        method_index[xrdinfo.stringify(method)] = 'REST'
                    else:
                        LOGGER.info('%s: %s', xrdinfo.stringify(method), err)
                        method_index[xrdinfo.stringify(method)] = ''
                    continue

                wsdl_name, hashes = save_wsdl(wsdl_path, hashes, wsdl, params['wsdl_replaces'])
                txt = '{}'.format(wsdl_name)
                try:
                    for wsdl_method in xrdinfo.wsdl_methods(wsdl):
                        method_full_name = xrdinfo.stringify(subsystem + wsdl_method)
                        method_index[method_full_name] = '{}/{}'.format(wsdl_rel_path, wsdl_name)
                        txt = txt + '\n    {}'.format(method_full_name)
                except xrdinfo.XrdInfoError as err:
                    txt = txt + '\nWSDL parsing failed: {}'.format(err)
                    method_index[xrdinfo.stringify(method)] = ''
                LOGGER.info(txt)

                if xrdinfo.stringify(method) not in method_index:
                    LOGGER.warning(
                        '%s - Method was not found in returned WSDL!', xrdinfo.stringify(method))
                    method_index[xrdinfo.stringify(method)] = ''

            with params['results_lock']:
                params['results'][wsdl_rel_path] = {
                    'methods': method_index,
                    'ok': True}
        except xrdinfo.XrdInfoError as err:
            with params['results_lock']:
                params['results'][wsdl_rel_path] = {
                    'methods': {},
                    'ok': False}
            LOGGER.info('%s: %s', xrdinfo.stringify(subsystem), err)
        except Exception as err:
            with params['results_lock']:
                params['results'][wsdl_rel_path] = {
                    'methods': {},
                    'ok': False}
            LOGGER.warning('Unexpected exception: %s: %s', type(err).__name__, err)
        finally:
            params['work_queue'].task_done()


def sort_by_time(item):
    """A helper function for sorting, indicates which field to use"""
    return item['reportTime']


def process_results(params):
    """Process results collected by worker threads"""
    results = params['results']

    card_nr = 0
    json_data = []
    for subsystem_key in sorted(results.keys()):
        card_nr += 1
        subsystem_result = results[subsystem_key]  # type: dict
        methods = subsystem_result['methods']
        if subsystem_result['ok'] and methods:
            subsystem_status = 'ok'
        elif subsystem_result['ok']:
            subsystem_status = 'empty'
        else:
            subsystem_status = 'error'
        subsystem = subsystem_key.split('/')
        json_subsystem = {
            'xRoadInstance': subsystem[0],
            'memberClass': subsystem[1],
            'memberCode': subsystem[2],
            'subsystemCode': subsystem[3],
            'subsystemStatus': 'ERROR' if subsystem_status == 'error' else 'OK',
            'methods': []
        }
        for method_key in sorted(methods.keys()):
            method = method_key.split('/')
            json_method = {
                'serviceCode': method[4],
                'serviceVersion': method[5],
            }
            if methods[method_key] == 'SKIPPED':
                json_method['methodStatus'] = 'SKIPPED'
                json_method['wsdl'] = ''
            elif methods[method_key] == 'TIMEOUT':
                json_method['methodStatus'] = 'TIMEOUT'
                json_method['wsdl'] = ''
            elif methods[method_key] == 'REST':
                json_method['methodStatus'] = 'REST'
                json_method['wsdl'] = ''
            elif methods[method_key]:
                json_method['methodStatus'] = 'OK'
                json_method['wsdl'] = methods[method_key]
            else:
                json_method['methodStatus'] = 'ERROR'
                json_method['wsdl'] = ''

            json_subsystem['methods'].append(json_method)
        json_data.append(json_subsystem)

    report_time = time.localtime(time.time())
    formatted_time = time.strftime('%Y-%m-%d %H:%M:%S', report_time)
    suffix = time.strftime('%Y%m%d%H%M%S', report_time)

    # JSON output
    with open('{}/index_{}.json'.format(params['path'], suffix), 'w') as json_file:
        json.dump(json_data, json_file, indent=2, ensure_ascii=False)

    json_history = []
    try:
        with open('{}/history.json'.format(params['path']), 'r') as json_file:
            json_history = json.load(json_file)
    except IOError:
        LOGGER.info('History file history.json not found')

    json_history.append({'reportTime': formatted_time, 'reportPath': 'index_{}.json'.format(
        suffix)})
    json_history.sort(key=sort_by_time, reverse=True)

    with open('{}/history.json'.format(params['path']), 'w') as json_file:
        json.dump(json_history, json_file, indent=2, ensure_ascii=False)

    # Replace index.json with latest report
    shutil.copy(
        '{}/index_{}.json'.format(params['path'], suffix), '{}/index.json'.format(params['path']))


def main():
    """Main function"""
    logging.config.dictConfig(DEFAULT_LOGGER)

    parser = argparse.ArgumentParser(
        description='Collect WSDL service descriptions from X-Road members.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS certificate is not validated.'
    )
    parser.add_argument(
        'config', metavar='CONFIG_FILE',
        help='Configuration file')
    args = parser.parse_args()

    config = load_config(args.config)
    if config is None:
        exit(1)

    configure_logging(config)

    params = set_params(config)
    if params is None:
        exit(1)

    if not make_dirs(params['path']):
        exit(1)

    shared_params = None
    try:
        shared_params = xrdinfo.shared_params_ss(
            addr=params['url'], instance=params['instance'], timeout=params['timeout'],
            verify=params['verify'], cert=params['cert'])
    except xrdinfo.XrdInfoError as err:
        LOGGER.error('Cannot download Global Configuration: %s', err)
        exit(1)

    # Create and start new threads
    threads = []
    for _ in range(params['thread_cnt']):
        thread = Thread(target=worker, args=(params,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Populate the queue
    try:
        for subsystem in xrdinfo.registered_subsystems(shared_params):
            if subsystem[2] in params['excluded_member_codes']:
                LOGGER.info('Skipping excluded member %s', xrdinfo.stringify(subsystem))
                continue
            if [subsystem[2], subsystem[3]] in params['excluded_subsystem_codes']:
                LOGGER.info('Skipping excluded subsystem %s', xrdinfo.stringify(subsystem))
                continue
            params['work_queue'].put(subsystem)
    except xrdinfo.XrdInfoError as err:
        LOGGER.error('Cannot process Global Configuration: %s', err)
        exit(1)

    # Block until all tasks in queue are done
    params['work_queue'].join()

    # Set shutdown event and wait until all daemon processes finish
    params['shutdown'].set()
    for thread in threads:
        thread.join()

    process_results(params)


if __name__ == '__main__':
    main()
