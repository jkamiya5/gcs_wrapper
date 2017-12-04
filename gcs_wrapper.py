import json
import time
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

  def __init__(self, project_id, engine_id, api_key):
    self.project_id = project_id
    self.custom_search_engine_id = engine_id
    self.custom_search_api_key = api_key
    self.custom_search_url = "https://www.googleapis.com/customsearch/v1?"

  def query(self, query_params, max_num=10, **arguments):

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
        logger.debug("payload:" + str(payload))
        res = requests.get(url=self.custom_search_url, params=payload, **arguments).content

        # logger.debug("res:" + str(res))
        data = json.loads(res.decode('utf-8'))

        if "items" not in data:
          try:
            info = json.loads(str(res.decode('utf-8')))
            if "error" not in info:
              break
            err_reason = info["error"]["errors"][0]["reason"]
            logger.debug("reason:" + str(err_reason))
            if err_reason == "dailyLimitExceeded":
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

  def query_image_urls(self, search_key, image_size="large", wait_for_proc=3, max_num=10, max_retry=3, **arguments):

    params = {}
    params["q"] = search_key
    params["searchType"] = "image"
    params["imgSize"] = image_size
    retry = 0
    max_retry = (max_retry if max_retry < 5 else 5)
    while (retry < max_retry):

      result = self.query(params, max_num=max_num, **arguments)
      if result == 2:
        logger.debug("LimitExceeded error. wait for proc")
        # print("LimitExceeded error. waiting for proc " + str(wait_for_proc) + " seconds")
        retry += 1
        time.sleep(wait_for_proc)
        continue

      elif result == 1:
        logger.debug("dailyLimitExceeded error. proc done")
        return None

      elif result is not None and isinstance(result, list) and len(result) != 0:
        iamge_urls = [x["link"] for x in result]
        return iamge_urls

    return None

  def query_image_urls_multiple_keys(self, search_keys, max_num=10, **arguments):
    result = []
    div = int(max_num / len(search_keys)) + 1
    # print(div)
    if search_keys is not None and isinstance(search_keys, list):
      for k in search_keys:
        urls = self.query_image_urls(search_key=k, max_num=div, **arguments)
        if urls is None or len(urls) == 0:
          continue
        result.extend(urls[:div])
    ret = result[:max_num]
    # print(len(result))
    # print(len(ret))
    return ret
