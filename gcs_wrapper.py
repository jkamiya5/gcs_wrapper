import copy
import json
import sys
import time
import traceback
from enum import Enum
from logging import DEBUG, getLogger

import requests
import six
from google.cloud import language
from google.cloud.language import enums, types

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
    self.language_client = language.LanguageServiceClient()
    self.cse_list = [
        "c2coff", "cr", "cx", "dateRestrict", "exactTerms", "excludeTerms", "fileType", "filter", "gl", "googlehost",
        "highRange", "hl", "hq", "imgColorType", "imgDominantColor", "imgSize", "imgType", "linkSite", "lowRange",
        "num", "orTerms", "relatedSite", "rights", "safe", "searchType", "siteSearch", "siteSearchFilter", "sort",
        "start"
    ]

  def query(self, search_key, max_num=10, standardize_search_keyword=False, **arguments):
    q = self.parse_search_key(search_key, standardize_search_keyword)
    payload = {}
    payload["q"] = q
    payload.update({key: value for key, value in arguments.items() if key in self.cse_list})
    arguments_ = {key: value for key, value in arguments.items() if key not in self.cse_list}
    payload["key"] = self.custom_search_api_key
    payload["cx"] = self.custom_search_engine_id
    result = []
    i = 0
    max_num = (max_num if max_num < 100 else 100)
    while i < max_num:
      try:
        payload["start"] = str(i + 1)
        payload["num"] = str(10 if (max_num - i) > 10 else (max_num - i))
        res = requests.get(url=self.custom_search_url, params=payload, **arguments_).content
        data = json.loads(res.decode('utf-8'))

        if "items" not in data:
          try:
            info = json.loads(str(res.decode('utf-8')))
            # print("info:" + str(info))
            if "error" not in info:
              break
            err_reason = info["error"]["errors"][0]["reason"]
            # logger.debug("reason:" + str(err_reason))
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

    # logger.debug("result_len:" + str(len(result[:max_num])))
    return result[:max_num]

  def query_image_urls(self, search_key, colname="link", max_retry=3, wait_for_proc=3, **arguments):
    retry = 0
    max_retry = (max_retry if max_retry < 5 else 5)
    while (retry < max_retry):

      result = self.query(search_key=search_key, searchType="image", imgSize="large", **arguments)
      if result == 2:
        logger.debug("LimitExceeded error. wait for proc")
        print("LimitExceeded error. waiting for proc " + str(wait_for_proc) + " seconds")
        retry += 1
        time.sleep(wait_for_proc)
        continue

      elif result == 1:
        logger.debug("dailyLimitExceeded error. proc done")
        return None

      elif result is not None and isinstance(result, list) and len(result) != 0:
        iamge_urls = None
        if colname == "link":
          iamge_urls = [x["link"] for x in result]
        elif colname == "thumbnailLink":
          iamge_urls = [x["image"]["thumbnailLink"] for x in result]
        return iamge_urls

    return None

  def query_image_urls_multiple_keys(self, search_keys, max_num=10, **arguments):
    result = []
    div = int(max_num / len(search_keys)) + 1
    if search_keys is not None and isinstance(search_keys, list):
      for k in search_keys:
        urls = self.query_image_urls(search_key=k, max_num=div, **arguments)
        if urls is None or len(urls) == 0:
          continue
        result.extend(urls[:div])
    ret = result[:max_num]
    return ret

  def query_image_thumbnail_urls(self, **arguments):
    return self.query_image_urls(colname="thumbnailLink", **arguments)

  def query_image_thumbnail_urls_multiple_keys(self, **arguments):
    return self.query_image_urls_multiple_keys(colname="thumbnailLink", **arguments)

  def entities_text(self, text):
    if isinstance(text, six.binary_type):
      text = text.decode('utf-8')
    document = types.Document(language="ja", content=text, type=enums.Document.Type.PLAIN_TEXT)
    entities = self.language_client.analyze_entities(document).entities
    val = " ".join([x.name for x in entities])
    return val

  def parse_search_key(self, search_key, standardize_search_keyword=False):
    search_key_ = search_key.replace("　", " ")
    if standardize_search_keyword:
      val = self.entities_text(search_key_)
      if val is not None:
        return val
    return search_key_
