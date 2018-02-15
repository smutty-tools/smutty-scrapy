# smutty-scrapy

python scraper and exporter for smutty.com metadata

# install

    sudo apt-get install \
      --no-install-recommends \
      --no-install-suggests \
      git gcc python3-venv python3-dev

    git clone https://github.com/smutty-tools/smutty-tools.git
    cd smutty-tools

    python3 -m venv venv
    venv/bin/pip3 install wheel
    venv/bin/pip3 install -r requirements.txt

For developpement using SublimeText, you can install these modules too :

    venv/bin/pip3 install flake8

# invocations via CRON

When running the script via cron, ensure that only one runs at a time :

    flock -x -n /tmp/smutty.lock ...

# configuration

A sample configuration file is in `config/smutty.conf`

Be sure to configure the database connection according to your setup

# scaper

This tool gets web pages from the site, extracts metadata and flushes the metadata into the database

    venv/bin/python3 -m smutty.scraper -h

This module uses state files, to track progression between runs :

- `current_scraper_page` marks the current scraped page, updated per page
- `highest_scraper_id` marks the first id seen in an still-unfinished run
- `lowest_exporter_id` state marks the highest id available for export, updated once the run is finished

# exporter

This tool extracts metadata from the database, splits and packages it in statically defined and compressed files

    venv/bin/python3 -m smutty.exporter -h

This module uses state files, to track progression between runs :

- `lowest_exporter_id` and `highest_exporter_id` states mark the range for which the export is complete

Export expansion is done at the boundaries, for efficiency

# initial database setup

The database schema name is currently fixed to `smutty` and is not configurable
on command line, see the first lines of `smutty/models.py` if you really need to
change it.

To setup your database, on a fresh Debian/Stretch system, install software :

    sudo apt-get install postgresql postgresql-client --no-install-recommends

If you plan to connect to the database from another host, reconfigure postgresql :

    sudo -i -u postgres
    # allow remote connections
    for MASK in "0.0.0.0/0" "::/0"; do
        echo "host all all ${MASK} md5"
    done >> /etc/postgresql/*/main/pg_hba.conf
    # listen on external addresses
    sed -i -e \
        's/^.*\b\(listen_addresses\)\b.*=.*$/\1 = '\''*'\''/' \
        /etc/postgresql/*/main/postgresql.conf
    # exit postgres user
    exit

    # apply new configuration
    sudo systemctl restart postgresql.service

Create a database user, and a database for this user :

    sudo -i -u postgres
    echo "CREATE USER smuttyuser WITH LOGIN ENCRYPTED PASSWORD 'smuttypassword';
    CREATE DATABASE smuttydb OWNER smuttyuser;" | psql

Create a schema for the data in the database :

    echo "CREATE SCHEMA smutty;" | psql -h localhost -U smuttyuser -d smuttydb
