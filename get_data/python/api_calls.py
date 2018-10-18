import os
import sys
import json
import logging
import logging.config
from time import sleep

import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from api_keys import API_ID, API_KEY
from constants import PROJECT_DIR

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def requests_retry_session(
    retries=10,
    backoff_factor=0.1,
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_api_call(url_extension):
    HOST = 'http://service.instatfootball.com/feed.php?id={}&key={}'.format(
        API_ID, API_KEY
    )
    url = HOST + url_extension

    try:
        r = requests_retry_session().get(url)
        json_data = json.loads(r.text)
        return json_data
    except Exception:
        logger.info('Retrying request')
        sleep(5)
        return get_api_call(url_extension)


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    from datetime import date

    from constants import GAME_INFO_URL

    sd = date(2018, 10, 7)
    ed = date(2018, 10, 9)
    game_id = '1234771'
    TEAMS_TPL = 42
    tournament_id = 93
    season_id = 22

    url = GAME_INFO_URL.format(TEAMS_TPL, game_id)

    test = get_api_call(url)['data']

    import pdb; pdb.set_trace()  # noqa # yapf: disable
    # print(test)
