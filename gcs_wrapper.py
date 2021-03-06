import copy
import json
import multiprocessing
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
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
            "q", "c2coff", "cr", "cx", "dateRestrict", "exactTerms", "excludeTerms", "fileType", "filter", "gl",
            "googlehost", "highRange", "hl", "hq", "imgColorType", "imgDominantColor", "imgSize", "imgType", "linkSite",
            "lowRange", "num", "orTerms", "relatedSite", "rights", "safe", "searchType", "siteSearch", "siteSearchFilter",
            "sort", "start"
        ]

    def parse_args(self, max_num, standardize_search_keyword, arguments):
        payload = {}
        gcs_params = {key: value for key,
                      value in arguments.items() if key in self.cse_list}
        payload.update(gcs_params)
        q = self.parse_search_key(gcs_params["q"], standardize_search_keyword)
        payload["q"] = q
        arguments_ = {key: value for key,
                      value in arguments.items() if key not in self.cse_list}
        payload["key"] = self.custom_search_api_key
        payload["cx"] = self.custom_search_engine_id
        max_num = (max_num if max_num < 100 else 100)
        return max_num, payload, arguments_

    def query(self, max_num=10, mode="normal", standardize_search_keyword=False, **arguments):
        if mode == "normal":
            return self.query_normal(max_num, standardize_search_keyword, **arguments)
        elif mode == "multithread":
            return self.query_multithread(max_num, standardize_search_keyword, **arguments)
        logger.debug("mode error")
        return None

    def query_normal(self, max_num, standardize_search_keyword, **arguments):
        max_num, payload, arguments_ = self.parse_args(
            max_num, standardize_search_keyword, arguments)
        i = 0
        result = []
        while i < max_num:
            payload["start"] = str(i + 1)
            payload["num"] = str(10 if (max_num - i) > 10 else (max_num - i))
            val = self.requests_get_data(payload, **arguments_)
            if val in (1, 2):
                return val
            result.extend(val)
            if len(val) < 10:
                break
            i = i + 10
        return result[:max_num]

    def query_multithread(self, max_num, standardize_search_keyword, **arguments):
        max_num, payload, arguments_ = self.parse_args(
            max_num, standardize_search_keyword, arguments)
        i = 0
        executer = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        futures = []
        while i < max_num:
            payload["start"] = str(i + 1)
            payload["num"] = str(10 if (max_num - i) > 10 else (max_num - i))
            payload_ = copy.deepcopy(payload)
            future = executer.submit(
                self.requests_get_data, payload_, **arguments_)
            futures.append(future)
            i = i + 10

        result = []
        for index, future in enumerate(futures):
            val, err_reason = future.result()
            if val in (1, 2):
                return {"val": val, "err_reason": err_reason}
            result.extend(val)
        # return result[:max_num], None
        return {"val": result[:max_num], "err_reason": None}

    def requests_get_data(self, payload, **arguments):
        try:
            res = requests.get(url=self.custom_search_url,
                               params=payload, **arguments).content
            data = json.loads(res.decode('utf-8'))
            if "items" not in data:
                try:
                    info = json.loads(str(res.decode('utf-8')))
                    if "error" not in info:
                        return [], None
                    err_reason = info["error"]["errors"][0]["reason"]
                    if err_reason == "dailyLimitExceeded":
                        return ERROR.STOP.value, err_reason
                    return ERROR.WAIT.value, err_reason
                except:
                    traceback.print_exc()
            return data["items"], None

        except requests.exceptions.HTTPError as e:
            logger.debug("e:" + str(e))
            err_data = e.content().decode('utf-8')
            logger.debug("err_data:" + str(err_data))
            err_data_ = json.loads(err_data)
            err_reason = err_data_["error"]["errors"][0]["reason"]
            if err_reason == "dailyLimitExceeded":
                return ERROR.STOP.value, err_reason
            return ERROR.WAIT.value

        except requests.exceptions.SSLError as e2:
            logger.debug("e:" + str(e2))
            return ERROR.STOP.value, "error"

        except:
            traceback.print_exc()
            return ERROR.STOP.value, "error"

        return []

    def query_image_urls(self, colname="link", max_retry=3, wait_for_proc=3, **arguments):
        retry = 0
        max_retry = (max_retry if max_retry < 5 else 5)
        while (retry < max_retry):

            result = self.query(searchType="image",
                                imgSize="large", **arguments)
            if result == 2:
                logger.debug("LimitExceeded error. wait for proc")
                print("LimitExceeded error. waiting for proc " +
                      str(wait_for_proc) + " seconds")
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
                iamge_urls_ = list(set(iamge_urls))
                return iamge_urls_

        return None

    def query_image_info(self, add_searchType_none=False, max_retry=3, wait_for_proc=3, **arguments):
        retry = 0
        max_retry = (max_retry if max_retry < 5 else 5)
        while (retry < max_retry):

            result = self.query(searchType="image", **arguments)
            result2 = None

            if add_searchType_none:
                result2 = self.query(**arguments)

            if result == 2 or result2 == 2:
                logger.debug("LimitExceeded error. wait for proc")
                print("LimitExceeded error. waiting for proc " +
                      str(wait_for_proc) + " seconds")
                retry += 1
                time.sleep(wait_for_proc)
                continue

            elif result == 1 or result2 == 1:
                logger.debug("dailyLimitExceeded error. proc done")
                return None

            else:
                ret = []
                if result is not None and isinstance(result, list) and len(result) != 0:
                    for x in result:
                        d = {
                            "link": x["link"],
                            "thumbnailLink": x["image"]["thumbnailLink"],
                            "contextLink": x["image"]["contextLink"],
                            "type": x["mime"],
                            "snippet": x["snippet"],
                            "title": x["title"]
                        }
                        ret.append(d)

                ret2 = []
                if result2 is not None and isinstance(result2, list) and len(result2) != 0:
                    for x in result2:
                        if "pagemap" not in x or "cse_image" not in x["pagemap"] or "cse_thumbnail" not in x["pagemap"]:
                            continue
                        d = {
                            "link":
                            x["pagemap"]["cse_image"][0]["src"],
                            "thumbnailLink":
                            x["pagemap"]["cse_thumbnail"][0]["src"],
                            "contextLink":
                            x["link"],
                            "type":
                            x["pagemap"]["metatags"][0]["og:type"]
                            if "metatags" in x["pagemap"] and "og:type" in x["pagemap"]["metatags"][0] else None,
                            "snippet":
                            x["snippet"],
                            "title":
                            x["title"]
                        }
                        ret2.append(d)

                if add_searchType_none:
                    num = int(len(result) / 2)
                    ret3 = []
                    ret3.extend(ret[:num])
                    ret3.extend(ret2[:num])
                    while len(ret3) < len(result):
                        ret3.append(ret[num])
                        num += 1
                    return ret3

                return ret

        return None

    def query_image_urls_multiple_keys(self, search_keys, max_num=10, **arguments):
        result = []
        div = int(max_num / len(search_keys)) + 1
        arguments_ = copy.deepcopy(arguments)
        if search_keys is not None and isinstance(search_keys, list):
            for k in search_keys:
                arguments_["q"] = k
                urls = self.query_image_urls(max_num=div, **arguments_)
                if urls is None or len(urls) == 0:
                    continue
                result.extend(urls[:div])
        result_ = list(set(result))
        return result_[:max_num]

    def query_image_info_multiple_keys(self, search_keys, max_num=10, **arguments):
        result = []
        div = int(max_num / len(search_keys)) + 1
        arguments_ = copy.deepcopy(arguments)
        if search_keys is not None and isinstance(search_keys, list):
            for k in search_keys:
                arguments_["q"] = k
                info = self.query_image_info(max_num=div, **arguments_)
                if info is None or len(info) == 0:
                    continue
                result.extend(info[:div])
        return result[:max_num]

    def query_image_thumbnail_urls(self, **arguments):
        return self.query_image_urls(colname="thumbnailLink", **arguments)

    def query_image_thumbnail_urls_multiple_keys(self, **arguments):
        return self.query_image_urls_multiple_keys(colname="thumbnailLink", **arguments)

    def entities_text(self, text):
        if isinstance(text, six.binary_type):
            text = text.decode('utf-8')
        document = types.Document(
            language="ja", content=text, type=enums.Document.Type.PLAIN_TEXT)
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
