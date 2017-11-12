# smutty-scrapy

python scrapper to get site metadata into a postgresql database

# install

    sudo apt-get install \
      --no-install-recommends \
      --no-install-suggests \
      git gcc python3-venv python3-dev

    git clone https://github.com/smutty-tools/smutty-scrapy.git
    cd smutty-scrapy

    python3 -m venv venv
    venv/bin/pip3 install wheel
    venv/bin/pip3 install -r requirements.txt

# run

    venv/bin/python3 run.py [options]

# options

    optional arguments:
        -h
        --help
            show this help message and exit

        -s N
        --start-page N
            starts at given page N

            Default: 1

        -c M
        --page-count M
            only scrap M pages

        -m [ID]
        --min-id [ID]
            fetches pages and items until given id is reached

            Default (if -m not provided): no id limit
            Default (if -m provided): get latest DB id and use it as limit

            NOTE: the 'new' channel provides id in descending order

        -b FILE
        --blacklist-tag-file FILE
            ignore items which have tags in provided list
            FILE holds a list of blacklisted tags, one word per line

        -d CONFIG [CONFIG ...]
        --database CONFIG [CONFIG ...]
            a set of key=value defining database parameters

# examples

Fetching first page only :

    venv/bin/python3 run.py -c 1 -d ...

Resume crawling at a given page :

    venv/bin/python3 run.py -s 123456 -d ...

Fetch newest posts since last run :

    venv/bin/python3 run.py -m -d ...

When running the script via cron, ensure that only one runs at a time :

    flock -x -n /tmp/smutty.lock ...

# database connection

All of the developpement and testing has been done on Postgresql 9.6,
and the script uses sqlalchemy, which can use other engines.

To provide connection information, use the `-d` argument with multiple
`CONFIG` arguments, where each `CONFIG` is in `key=value` format.

In the example below, capitalized words are placeholders for your real information :

    ... -d drivername=postgres host=ADDRESS port=5432 \
       username=LOGIN password=PASSWORD database=DBNAME

See [this SqlAlchemy documentation](http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.engine.url.URL)
for more information on database connection settings.

# database setup

The database schema name is currently fixed to `smutty` and is not configurable
on command line, see the first lines of `models.py` if you really need to
change it.

To setup your database, on a fresh Debian/Strech system, install software :

    sudo apt-get install postgresql postgresql-client --no-install-recommends

If you plan to connect to the database from another host, reconfigure postgresql :

    su - postgres

    # allow remote connections
    for MASK in "0.0.0.0/0" "::/0"; do
        echo "host all all ${MASK} md5"
    done >> /etc/postgresql/*/main/pg_hba.conf

    # listen on external addresses
    sed -i -e \
        's/^.*\b\(listen_addresses\)\b.*=.*$/\1 = '\''*'\''/' \
        /etc/postgresql/*/main/postgresql.conf

    exit

    # apply new configuration
    sudo systemctl restart postgresql.service

Create a database user, and a database for this user :

    su - postgres

    echo "CREATE USER smuttyuser WITH LOGIN ENCRYPTED PASSWORD 'smuttypassword';
    CREATE DATABASE smuttydb OWNER smuttyuser;" | psql

Create a schema for the data in the database :

    echo "CREATE SCHEMA smutty;" | psql -U smuttyuser -d smuttydb
