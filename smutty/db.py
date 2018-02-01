import sqlalchemy


class DatabaseConfiguration:

    def __init__(self, config_section):
        # read provided configuration
        self._dialect = config_section['dialect']
        self._username = config_section['username']
        self._password = config_section['password']
        self._host = config_section['host']
        self._port = int(config_section['port'])
        self._database = config_section['database']

    @property
    def url(self):
        return sqlalchemy.engine.url.URL(
            drivername=self._dialect,
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            database=self._database
        )


class DatabaseSession:

    def __init__(self, url):
        self._url = url

        # initialize engine, session class, and shared session
        self._engine = sqlalchemy.create_engine(self._url)
        self._session_factory = sqlalchemy.orm.sessionmaker(bind=self._engine)
        self._session = self._session_factory()

    @property
    def session(self):
        return self._session

    @property
    def engine(self):
        return self._engine
