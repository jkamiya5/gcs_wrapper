import json
import traceback
from enum import Enum
from logging import DEBUG, getLogger

import requests

logger = getLogger(__name__)
logger.setLevel(DEBUG)
logger.propagate = False


class ERROR(Enum):
  STOP = 1
  WAIT = 2


class GcsWrapper(object):

  __instance = None

  def __new__(cls, *args, **keys):
    if cls.__instance is None:
      cls.__instance = object.__new__(cls)
    return cls.__instance

  def __init__(self, project_id, engine_id, api_key, proxies=None, headers=None):
    self.project_id = project_id
    self.custom_search_engine_id = engine_id
    self.custom_search_api_key = api_key
    self.custom_search_url = "https://www.googleapis.com/customsearch/v1?"
    self.custom_search_proxies = proxies
    self.custom_search_headers = headers

  def query(self, query_params, proxies=None, headers=None, max_num=10):

    if proxies is not None:
      self.custom_search_proxies = proxies

    if headers is not None:
      self.custom_search_headers = headers

    result = []
    payload = {}
    payload.update(query_params)
    payload["key"] = self.custom_search_api_key
    payload["cx"] = self.custom_search_engine_id
    i = 0

    while i < max_num:

      try:

        payload["start"] = str(i + 1)
        payload["num"] = str(10 if (max_num - i) > 10 else (max_num - i))
        # logger.debug("payload:" + str(payload))

        res = requests.get(
            self.custom_search_url,
            params=payload,
            proxies=self.custom_search_proxies,
            headers=self.custom_search_headers).content

        # logger.debug("res:" + str(res))
        data = json.loads(res.decode('utf-8'))

        if "items" not in data:
          try:
            err_data = json.loads(str(res.decode('utf-8')))
            if err_data["error"]["errors"][0]["reason"] == "dailyLimitExceeded":
              return ERROR.STOP.value
            return ERROR.WAIT.value
          except:
            traceback.print_exc()
            break

        result.extend(data["items"])

        if len(data["items"]) < 10:
          break

        i = i + 10

      except requests.exceptions.HTTPError as e:
        logger.debug("e:" + str(e))
        err_data = e.content().decode('utf-8')
        logger.debug("err_data:" + str(err_data))
        err_data_ = json.loads(err_data)
        if err_data_["error"]["errors"][0]["reason"] == "dailyLimitExceeded":
          return ERROR.STOP.value
        return ERROR.WAIT.value

      except requests.exceptions.SSLError as e2:
        logger.debug("e:" + str(e2))
        return ERROR.STOP.value

      except:
        traceback.print_exc()
        return ERROR.STOP.value

    logger.debug("result_len:" + str(len(result)))
    return result
