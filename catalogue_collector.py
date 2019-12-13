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
import urllib.parse as urlparse
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


def identifier_path(items):
    """Convert identifier in form of list/tuple to string representation
    of filesystem path. We assume that no symbols forbidden by
    filesystem are used in identifiers.
    """
    return '/'.join(items)


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


def hash_openapis(path):
    """Find hashes of all OpenAPI documents in directory"""
    hashes = {}
    for file_name in os.listdir(path):
        search_res = re.search('^.+_(\\d+)\\.(yaml|json)$', file_name)
        if search_res:
            # Reading as bytes to avoid line ending conversion
            with open('{}/{}'.format(path, file_name), 'rb') as openapi_file:
                openapi = openapi_file.read()
            hashes[file_name] = hashlib.md5(openapi).hexdigest()
    return hashes


def save_openapi(path, hashes, wsdl, service_name, doc_type):
    """Save OpenAPI if it does not exist yet"""
    openapi_hash = hashlib.md5(wsdl.encode('utf-8')).hexdigest()
    max_openapi = -1
    for file_name in hashes.keys():
        search_res = re.search('^{}_(\\d+)\\.(yaml|json)$'.format(service_name), file_name)
        if search_res:
            if openapi_hash == hashes[file_name]:
                # Matching OpenAPI found (both name pattern and hash)
                return file_name, hashes
            if int(search_res.group(1)) > max_openapi:
                max_openapi = int(search_res.group(1))
    # Creating new file
    new_file = '{}_{}.{}'.format(service_name, int(max_openapi) + 1, doc_type)
    # Writing as bytes to avoid line ending conversion
    with open('{}/{}'.format(path, new_file), 'wb') as openapi_file:
        openapi_file.write(wsdl.encode('utf-8'))
    hashes[new_file] = openapi_hash
    return new_file, hashes


def method_item(method, status, wsdl):
    """Function that sets the correct structure for method item"""
    return {
        'serviceCode': method[4],
        'serviceVersion': method[5],
        'methodStatus': status,
        'wsdl': wsdl
    }


def service_item(service, status, openapi, endpoints):
    """Function that sets the correct structure for service item
    If status=='OK' and openapi is empty then:
      * it is REST X-Road service that does not have a description;
      * endpoints array is empty.
    If status=='OK' and openapi is not empty then:
      * it is OpenAPI X-Road service with description;
      * at least one endpoint must be present in OpenAPI description.
    In other cases status must not be 'OK' to indicate problem with
    the service.
    """
    return {
        'serviceCode': service[4],
        'status': status,
        'openapi': openapi,
        'endpoints': endpoints
    }


def subsystem_item(subsystem, methods, services):
    """Function that sets the correct structure for subsystem item"""
    subsystem_status = 'ERROR'
    sorted_methods = []
    if methods is not None:
        subsystem_status = 'OK'
        for method_key in sorted(methods.keys()):
            sorted_methods.append(methods[method_key])

    return {
        'xRoadInstance': subsystem[0],
        'memberClass': subsystem[1],
        'memberCode': subsystem[2],
        'subsystemCode': subsystem[3],
        'subsystemStatus': subsystem_status,
        'servicesStatus': 'OK' if services is not None else 'ERROR',
        'methods': sorted_methods,
        'services': services if services is not None else []
    }


def process_methods(subsystem, params, doc_path):
    """Function that finds SOAP methods of a subsystem"""
    wsdl_path = '{}/{}'.format(params['path'], doc_path)
    try:
        make_dirs(wsdl_path)
        hashes = hash_wsdls(wsdl_path)
    except OSError as err:
        LOGGER.warning('SOAP: %s: %s', identifier_path(subsystem), err)
        return None

    method_index = {}
    skip_methods = False
    try:
        # Converting iterator to list to properly capture exceptions
        methods = list(xrdinfo.methods(
            addr=params['url'], client=params['client'], producer=subsystem,
            method='listMethods', timeout=params['timeout'], verify=params['verify'],
            cert=params['cert']))
    except xrdinfo.XrdInfoError as err:
        LOGGER.info('SOAP: %s: %s', identifier_path(subsystem), err)
        return None

    for method in sorted(methods):
        method_name = identifier_path(method)
        if method_name in method_index:
            # Method already found in previous WSDL's
            continue

        if skip_methods:
            # Skipping, because previous getWsdl request timed out
            LOGGER.info('SOAP: %s - SKIPPING', method_name)
            method_index[method_name] = method_item(method, 'SKIPPED', '')
            continue

        try:
            wsdl = xrdinfo.wsdl(
                addr=params['url'], client=params['client'], service=method,
                timeout=params['timeout'], verify=params['verify'], cert=params['cert'])
        except xrdinfo.RequestTimeoutError:
            # Skipping all following requests to that subsystem
            skip_methods = True
            LOGGER.info('SOAP: %s - TIMEOUT', method_name)
            method_index[method_name] = method_item(method, 'TIMEOUT', '')
            continue
        except xrdinfo.XrdInfoError as err:
            if str(err) == 'SoapFault: Service is a REST service and does not have a WSDL':
                # This is specific to X-Road 6.21 (partial and
                # deprecated support for REST). We do not want to spam
                # INFO messages about REST services
                LOGGER.debug('SOAP: %s: %s', method_name, err)
            else:
                LOGGER.info('SOAP: %s: %s', method_name, err)
            method_index[method_name] = method_item(method, 'ERROR', '')
            continue

        try:
            wsdl_name, hashes = save_wsdl(wsdl_path, hashes, wsdl, params['wsdl_replaces'])
        except OSError as err:
            LOGGER.warning('SOAP: %s: %s', method_name, err)
            method_index[method_name] = method_item(method, 'ERROR', '')
            continue

        txt = 'SOAP: {}'.format(wsdl_name)
        try:
            for wsdl_method in xrdinfo.wsdl_methods(wsdl):
                wsdl_method_name = identifier_path(subsystem + wsdl_method)
                # We can find other methods in a method WSDL
                method_index[wsdl_method_name] = method_item(
                    subsystem + wsdl_method, 'OK', urlparse.quote(
                        '{}/{}'.format(doc_path, wsdl_name)))
                txt = txt + '\n    {}'.format(wsdl_method_name)
        except xrdinfo.XrdInfoError as err:
            txt = txt + '\nWSDL parsing failed: {}'.format(err)
            method_index[method_name] = method_item(method, 'ERROR', '')
        LOGGER.info(txt)

        if method_name not in method_index:
            LOGGER.warning(
                'SOAP: %s - Method was not found in returned WSDL!', method_name)
            method_index[method_name] = method_item(method, 'ERROR', '')
    return method_index


def process_services(subsystem, params, doc_path):
    """Function that finds REST services of a subsystem"""
    openapi_path = '{}/{}'.format(params['path'], doc_path)
    try:
        make_dirs(openapi_path)
        hashes = hash_openapis(openapi_path)
    except OSError as err:
        LOGGER.warning('REST: %s: %s', identifier_path(subsystem), err)
        return None

    results = []
    skip_services = False

    try:
        # Converting iterator to list to properly capture exceptions
        services = list(xrdinfo.methods_rest(
            addr=params['url'], client=params['client'], producer=subsystem,
            method='listMethods', timeout=params['timeout'], verify=params['verify'],
            cert=params['cert']))
    except xrdinfo.XrdInfoError as err:
        LOGGER.info('REST: %s: %s', identifier_path(subsystem), err)
        return None

    for service in sorted(services):
        service_name = identifier_path(service)

        if skip_services:
            # Skipping, because previous getOpenAPI request timed out
            LOGGER.info('REST: %s - SKIPPING', service_name)
            results.append(service_item(service, 'SKIPPED', '', []))
            continue

        try:
            openapi = xrdinfo.openapi(
                addr=params['url'], client=params['client'], service=service,
                timeout=params['timeout'], verify=params['verify'], cert=params['cert'])
        except xrdinfo.RequestTimeoutError:
            # Skipping all following requests to that subsystem
            skip_services = True
            LOGGER.info('REST: %s - TIMEOUT', service_name)
            results.append(service_item(service, 'TIMEOUT', '', []))
            continue
        except xrdinfo.NotOpenapiServiceError:
            results.append(service_item(service, 'OK', '', []))
            continue
        except xrdinfo.XrdInfoError as err:
            LOGGER.info('REST: %s: %s', service_name, err)
            results.append(service_item(service, 'ERROR', '', []))
            continue

        try:
            _, openapi_type = xrdinfo.load_openapi(openapi)
            endpoints = xrdinfo.openapi_endpoints(openapi)
        except xrdinfo.XrdInfoError as err:
            LOGGER.info('REST: %s: %s', service_name, err)
            results.append(service_item(service, 'ERROR', '', []))
            continue

        try:
            openapi_name, hashes = save_openapi(
                openapi_path, hashes, openapi, service[4], openapi_type)
        except OSError as err:
            LOGGER.warning('REST: %s: %s', service_name, err)
            results.append(service_item(service, 'ERROR', '', []))
            continue

        results.append(
            service_item(service, 'OK', urlparse.quote(
                '{}/{}'.format(doc_path, openapi_name)), endpoints))

    return results


def worker(params):
    """Main function for worker threads"""
    while True:
        # Checking periodically if it is the time to gracefully shutdown
        # the worker.
        try:
            subsystem = params['work_queue'].get(True, 0.1)
            LOGGER.info('Start processing %s', identifier_path(subsystem))
        except queue.Empty:
            if params['shutdown'].is_set():
                return
            continue
        subsystem_path = ''
        try:
            subsystem_path = identifier_path(subsystem)
            methods_result = process_methods(subsystem, params, subsystem_path)
            services_result = process_services(subsystem, params, subsystem_path)

            with params['results_lock']:
                params['results'][subsystem_path] = subsystem_item(
                    subsystem, methods_result, services_result)
        # Using broad exception to avoid unexpected exits of workers
        except Exception as err:
            with params['results_lock']:
                params['results'][subsystem_path] = subsystem_item(subsystem, None, None)
            LOGGER.warning('Unexpected exception: %s: %s', type(err).__name__, err)
        finally:
            params['work_queue'].task_done()


def sort_by_time(item):
    """A helper function for sorting, indicates which field to use"""
    return item['reportTime']


def all_results_failed(subsystems):
    """Check if all results have failed status"""
    for subsystem in subsystems.values():
        if subsystem['subsystemStatus'] == 'OK':
            # Found non-failed subsystem
            return False
    # All results failed
    return True


def process_results(params):
    """Process results collected by worker threads"""
    results = params['results']

    if all_results_failed(results):
        # Skipping this version
        LOGGER.error('All subsystems failed, skipping this catalogue version!')
        return

    json_data = []
    for subsystem_key in sorted(results.keys()):
        json_data.append(results[subsystem_key])

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
                LOGGER.info('Skipping excluded member %s', identifier_path(subsystem))
                continue
            if [subsystem[2], subsystem[3]] in params['excluded_subsystem_codes']:
                LOGGER.info('Skipping excluded subsystem %s', identifier_path(subsystem))
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
