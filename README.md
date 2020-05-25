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

## Helper scripts

* `recreate_history.py` - This script can be used to to update history.json file when it was corrupted or when some of the reports were deleted. Usage: `python3 recreate_history.py <path to catalogue>`
* `remove_unused.py` - This script can be used to remove WSDL files that are no longer used in X-tee catalogue. For example due to deletion of older catalogue reports. Usage: `python3 remove_unused.py <path to catalogue>`. Or to simply list unused WSDLs: `python3 remove_unused.py --only-list <path to catalogue>`
* `clean_history.py` - This script can be used to remove old JSON index files to free up disk space. Second parameter is an ammount of latest days that will not be cleaned. Older days will be cleaned so that only the first report of the day is kept. Usage: `python3 clean_history.py <path to catalogue> <days to keep>`

If after usage of `remove_unused.py` you need to also delete empty directories then execute the following command inside catalogue directory:
```bash
find . -type d -empty -delete
```

## Minio storage

If `minio_url` is configured then collected data will be pushed to minio storage.

To test minio locally on linux machine execute the following commands (note that you should never use the default password for production):
```bash
sudo mkdir -p /mnt/data
docker run -d -p 9000:9000 --name minio1 -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" -v /mnt/data:/data minio/minio server /data
cd
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
~/mc config host add --quiet --api s3v4 cat http://localhost:9000 minioadmin minioadmin
~/mc mb cat/catalogue
~/mc policy set download cat/catalogue
```
