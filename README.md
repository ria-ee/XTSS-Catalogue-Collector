# Collector for X-tee subsystems and methods catalogue

This application will collect data for X-tee subsystems and methods catalogue. Collector will output collected data to a directory that is ready to by served by web server (like Apache or Nginx). Subsequent executions will create new versions of catalogue while preserving old versions.

## Configuration

Create a configuration file for your X-Road instance using an example configuration file: [example-config.json](example-config.json). If you need to provide catalogue data for multiple X-Road instances then you will need separate configurations for each X-Road instance.

Configuration parameters:
* `output_path` - output directory for collected data;
* `server_url` - address of your security server;
* `client` - array of X-Road client identifiers;
* `instance` - X-Road instance to collect data from;
* `timeout` - X-Road query timeout;
* `server_cert` - optional TLS certificate of your security server for verification;
* `client_cert` - optional application TLS certificate for authentication with security server;
* `client_key` - optional application key for authentication with security server;
* `thread_count` - amount of parallel threads to use;
* `wsdl_replaces` - replace metadata like creation timestamp in WSDLs to avoid duplicates;
* `excluded_member_codes` - exclude certain members who are permanently in faulty state or should not be queried for any other reasons;
* `excluded_subsystem_codes` - exclude certain members who are permanently in faulty state or should not be queried for any other reasons;
* `logging-config` - logging configuration passed to logging.config.dictConfig(). You can read more about Python3 logging here: [https://docs.python.org/3/library/logging.config.html](https://docs.python.org/3/library/logging.config.html).

## Installing python venv

Python virtual environment is an easy way to manage application dependencies. First You will need to install support for python venv:
```bash
sudo apt-get install python3-venv
```

Then install required python modules into venv:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

You can run the collector by issuing command (with activated venv):
```bash
python catalogue-collector.py config-instance1.json
```

## Systemd timer

Systemd timer can be used as more advanced version of cron. You can use provided example timer and service definitions to perform scheduled collection of data from your instances.

Add service description `systemd/catalogue-collector.service` to `/lib/systemd/system/catalogue-collector.service` and timer description `systemd/catalogue-collector.timer` to `/lib/systemd/system/catalogue-collector.timer`.

Then start and enable automatic startup:
```bash
sudo systemctl daemon-reload
sudo systemctl start catalogue-collector.timer
sudo systemctl enable catalogue-collector.timer
```
