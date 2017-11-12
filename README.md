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
