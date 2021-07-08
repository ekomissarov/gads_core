from __future__ import annotations

import googleads.errors
from googleads import adwords
from common_constants import constants
import re
import pickle
from datetime import date
from datetime import timedelta
from datetime import datetime
from google_analytics.analyticsbase import DateDeque
from time import sleep

import zeep
import zeep.helpers

from urllib3.exceptions import ProtocolError
from http.client import RemoteDisconnected
from googleapiclient.errors import HttpError
from socket import timeout

ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/gads_core/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class GoogleAdsError(constants.PySeaError): pass
class IntegrityDataError(GoogleAdsError): pass
class PeriodError(GoogleAdsError): pass
class LimitOfRetryError(GoogleAdsError): pass


def main_array_limit(nlim):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для генерации вызовов API с ограничением по количеству
    передаваемых CampaignIds, AdGroupIds и пр.
    Используется первый список передающийся в функцию

    :param nlim: количество CampaignIds в одном API запросе
    :return:
    """
    def deco_list_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, lst, *argp, **argn):  # конструируемая функция
            result_list = []
            result_dict = {}

            if type(lst) is str:
                lst = [int(lst)]
            elif type(lst) is int:
                lst = [lst]

            parts = (lst[i:i+nlim] for i in range(0, len(lst), nlim))  # chunk, разбивает список на части по ~nlim шт.

            for n, i in enumerate(parts):
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['part_num'] = n

                res = f(self, i, *argp, **argn)
                if type(res) is list:
                    result_list.extend(res)
                elif type(res) is dict:
                    result_dict.update(res)
                elif issubclass(type(res), zeep.helpers.CompoundValue):
                    result_list.append(zeep.helpers.serialize_object(res, dict))

            if result_dict:
                return result_dict
            elif result_list:
                return result_list
            else:
                return []
        return constructed_function
    return deco_list_limit


def dump_to(prefix, d=False):  # конструктор декоратора (n залипает в замыкании)
    """
    Декоратор для кеширования возврата функции.
    Применим к методам класса, в котором объявлены:
    self.directory - ссылка на каталог
    self.dump_file_prefix - файловый префикс
    self.cache - True - кеширование требуется / False
    На вход принимает префикс, который идентифицирует декорируемую функцию

    Кеш хранится в сериализованных файлах с помощью pickle

    :param prefix: идентифицирует декорируемую кешируемую функцию
    :param d: явно указанная дата в self.current_date или False для сегодняшней даты (для формирования имени файла)
    :return:
    """
    def deco_dump(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            if 'dump_parts_flag' in self.__dict__:
                dump_file_prefix = f"{self.dump_file_prefix}_p{self.dump_parts_flag['part_num']}"
            else:
                dump_file_prefix = self.dump_file_prefix

            if not d:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                     date.today()).replace("//", "/")
            else:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                     self.current_date).replace("//", "/")
            read_data = ""

            if self.cache:  # если кеширование требуется
                try:  # пробуем прочитать из файла
                    with open(file_out, "rb") as file:
                        read_data = pickle.load(file)
                except Exception as err:
                    logger.debug(f"{err}\n Cache file {file_out} is empty, getting fresh...")

            if not read_data:  # если не получилось то получаем данные прямым вызовом функции
                read_data = f(self, *argp, **argn)
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['len'] = len(read_data)

                with open(file_out, "wb") as file:  # записываем результат в файл
                    if 'dump_parts_flag' in self.__dict__:
                        pickle.dump(read_data[-self.dump_parts_flag['len']:], file, pickle.HIGHEST_PROTOCOL)
                    else:
                        pickle.dump(read_data, file, pickle.HIGHEST_PROTOCOL)
            return read_data
        return constructed_function
    return deco_dump


def connection_attempts(n=12, t=10):  # конструктор декоратора (N,T залипает в замыкании)
    """
    Декоратор задает n попыток для соединения с сервером в случае ряда исключений
    с задержкой t*2^i секунд

    :param n: количество попыток соединения с сервером [1, 15]
    :param t: количество секунд задержки на первой попытке попытке (на i'ом шаге t*2^i)
    :return:
    """
    def deco_connect(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(*argp, **argn):  # конструируемая функция
            retry_flag, pause_seconds = n, t
            try_number = 0

            if retry_flag < 0 or retry_flag > 15:
                retry_flag = 8
            if pause_seconds < 1 or pause_seconds > 30:
                pause_seconds = 10

            while True:
                try:
                    result = f(*argp, **argn)
                    # Обработка ошибки, если не удалось соединиться с сервером
                except (ConnectionError,
                        ProtocolError, RemoteDisconnected,
                        HttpError, timeout,
                        googleads.errors.GoogleAdsServerFault) as err:
                    logger.error(f"Ошибка соединения с сервером {err}. Осталось попыток {retry_flag - try_number}")
                    if try_number >= retry_flag:
                        raise LimitOfRetryError
                    sleep(pause_seconds * 2 ** try_number)
                    try_number += 1
                    continue
                else:
                    return result

            return None
        return constructed_function
    return deco_connect


def limit_by(nlim):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для использования постраничной выборки в вызовах API Google Adwords
    https://developers.google.com/adwords/api/docs/reference/v201809/DataService.Paging

    :param nlim: не более 10 000 объектов за один запрос. (для метода get)
    :return:
    """
    def deco_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            result = []
            self.PAGE_SIZE = nlim

            more_pages = True
            while more_pages:
                data = f(self, *argp, **argn)
                result.extend(data[0])

                self.offset += self.PAGE_SIZE
                more_pages = self.offset < data[1]

            self.offset = 0  # не забываем вернуть пагенатор в исходное состояние для следующих вызовов
            return result
        return constructed_function
    return deco_limit


class CSVReport:
    def __init__(self, csv: str = "") -> None:
        self.__csv = csv
        self.data = []
        self.report_name = ""
        self.period_begin = None
        self.period_end = None
        if csv:
            self._create_report_from_csv()

    def _create_report_from_csv(self) -> None:
        d = re.compile('\(([a-zA-Z]+\s[0-9]+,\s[0-9]{4})\-([a-zA-Z]+\s[0-9]+,\s[0-9]{4})\)')
        d_lite = re.compile('\(([a-zA-Z]+\s[0-9]+,\s[0-9]{4})\)')
        report = self.__csv.split("\n")

        period = d.search(report[0])
        if period is None:
            period = d_lite.search(report[0]).groups()
            self.period_begin, self.period_end = period[0], period[0]
        else:
            self.period_begin, self.period_end = period.groups()

        self.period_begin = datetime.date(datetime.strptime(self.period_begin, "%b %d, %Y"))
        self.period_end = datetime.date(datetime.strptime(self.period_end, "%b %d, %Y"))

        fields = report[1].split(",")
        self.report_name = report[0].split()[0].strip('"')

        impressions_index = report[1].split(",").index('Impressions')
        impressions_summ = 0
        for i in report[2:]:
            if i.find("Total,") != -1:
                if int(i.split(",")[impressions_index]) != impressions_summ:
                    logger.error("Проверка суммы Impressions: Вероятно нарушена целостность")
                    raise IntegrityDataError
                break
            impressions_summ += int(i.split(",")[impressions_index])
            line = dict(zip(fields, i.split(",")))  # получили dict с именами полей
            # приведение типов для извесных полей
            # https://support.google.com/google-ads/answer/7501826?hl=en
            # https://support.google.com/google-ads/answer/2497703?hl=en
            for field in ['Search top IS', 'Search abs. top IS', 'Search Impr. share',
                          'Impr. (Top) %', 'Impr. (Abs. Top) %']:
                if line.get(field, False):
                    if line[field].find("--") != -1:
                        line[field] = None
                    elif line[field].find('< 10%') != -1:
                        line[field] = 1.0
                    else:
                        line[field] = float(line[field].replace("%", ""))
            for field in ['Search top IS', 'Impr. (Top) %', 'Impr. (Abs. Top) %']:
                if line.get(field, False):
                    line[field] *= 100

            for field in ['Impressions', 'Clicks', 'Cost']:
                if line.get(field, False):
                    if line[field].find("-") != -1:
                        line[field] = 0
                    else:
                        line[field] = int(line[field])

            for field in ['Campaign ID', 'Ad group ID', 'Keyword ID']:
                if line.get(field, False):
                    line[field] = int(line[field])

            if line.get('Keyword / Placement', False):
                line['Keyword / Placement'] = line['Keyword / Placement'].strip('"')

            if line.get('Day', False):
                line['Day'] = date.fromisoformat(line['Day'])

            self.data.append(line)

    def search_field(self, field_name, field_value):
        if len(self.data) > 0:
            if self.data[0].get(field_name, "no_field") == "no_field":
                raise KeyError
            if type(self.data[0][field_name]) != type(field_value):
                raise TypeError

            for i in self.data:
                if i[field_name] == field_value:
                    return i
            else:
                raise IndexError

    def __str__(self) -> str:
        return f"Отчет Google Ads {self.report_name} за период {self.period_begin} - {self.period_end}"


class GAReportByDate(CSVReport):
    def __init__(self, csv: str = "") -> None:
        if type(csv) is str:
            super(GAReportByDate, self).__init__(csv)
        elif type(csv) is CSVReport:
            self.__dict__ = csv.__dict__

        self.ids_index = set()
        self.date_data = DateDeque()

        if self.data:
            self._create_date_report_from_data(self.data)

    def __getitem__(self, date_item: date) -> tuple:
        if type(date_item) is str:
            return self.date_data.get_by_date(date.fromisoformat(date_item))
        return self.date_data.get_by_date(date_item)

    def __iter__(self):
        """
        Объект иттерируем по датам отчета, при этом возвращается словарь с добавленной датой и идентификатором кампании
        :return: иттератор
        """
        for d in self.date_data:
            for j in d[1].items():  # d[0] - дата
                for k in j[1]:  # j[0] - CampaignId
                    # out = {"Date": d[0], "Day": d[0], "Campaign ID": j[0]}  # Day и Date для обратной совместимости
                    out = {"Day": d[0], "Campaign ID": j[0]}
                    out.update(k)
                    yield out

    def _create_date_report_from_data(self, d: list) -> None:
        if len(d) == 0:
            return None
        d.sort(key=lambda x: x['Day'], reverse=False)
        start_point = d[0]['Day']
        tmp_date = dict()

        for i in d:
            curr_date = i.pop('Day')
            curr_campaignid = i.pop('Campaign ID')

            if curr_date != start_point:
                self.date_data.append((start_point, tmp_date))
                start_point = curr_date
                tmp_date = dict()

            if curr_campaignid not in tmp_date:
                tmp_date.update({curr_campaignid: []})

            tmp_date[curr_campaignid].append(i)

        if tmp_date:
            self.date_data.append((start_point, tmp_date))

    def build_index(self) -> None:
        """
        создает индекс по идентификаторам (CampaignId, AdGroupId, AdGroupName, CriteriaId, Criteria)
        :return:
        """
        for d in self.date_data:
            for j in d[1].items():  # d[0] - дата
                for k in j[1]:  # j[0] - CampaignId
                    out = (j[0], k['Ad group ID'], k['Ad group'], k['Keyword ID'],
                           re.sub("\s\-.*$", "", k['Keyword / Placement'])  # подчищаем минус слова
                           )
                    self.ids_index.add(out)

    def set_begin_date(self, begin_date: date) -> None:
        self.period_begin = begin_date
        self.date_data.clear_dates_before(begin_date)

    def add_data(self, d: CSVReport) -> None:
        if self.report_name and self.report_name != d.report_name:
            logger.error(f"Тип присоединяемого отчета {d.report_name} не совпадает с {self.report_name}")
            raise IntegrityDataError

        if self.period_end and self.period_end != d.period_begin - timedelta(1):
            logger.error(f"Дата основной статистики заканчивается на {self.period_end}\n"
                         f"дата присоединяемого периода статистики начинается с {d.period_begin}")
            raise PeriodError

        self.period_end = d.period_end
        self._create_date_report_from_data(d.data)

    def summ_stat(self, from_date: date = False, to_date: date = False,
                  campaign_id: int = False, adgroup_id: int = False, criteria_id: int = False) -> dict:
        """
        подсчитывает статистику в виде {"from_date": from_date, "to_date": to_date, "AdGroupId": adgroup_id,
        "CriteriaId": criteria_id, "Impressions": 0, "Clicks": 0, "Cost": 0}

        :param from_date: дата в формате YYYY-MM-DD или class 'datetime.date'
        :param to_date: дата в формате YYYY-MM-DD или class 'datetime.date'
        :param campaign_id: идентификатор кампании
        :param adgroup_id: идентификатор группы
        :param criteria_id: идентификатор критерия таргетинга / ключевого слова
        :return: dict со статистикой
        """
        d = tuple(i[0] for i in self.date_data)
        from_date = min(d) if from_date is False else from_date
        to_date = max(d) if to_date is False else to_date
        from_date = date.fromisoformat(from_date) if type(from_date) is str else from_date
        to_date = date.fromisoformat(to_date) if type(to_date) is str else to_date
        if type(campaign_id) is not int:
            campaign_id = int(campaign_id)
        if type(adgroup_id) is not int:
            adgroup_id = int(adgroup_id)
        if type(criteria_id) is not int:
            criteria_id = int(criteria_id)

        period = tuple(from_date + timedelta(days=x) for x in range(0, (to_date-from_date).days + 1))

        result = {"from_date": from_date, "to_date": to_date,
                  "CampaignId": campaign_id, "AdGroupId": adgroup_id, "CriteriaId": criteria_id,
                  "Impressions": 0, "Clicks": 0, "Cost": 0}

        for i in self.date_data:
            if i[0] in period:
                for j in i[1].items():
                    if not campaign_id or campaign_id == j[0]:
                        for k in j[1]:
                            if not adgroup_id or adgroup_id == k['Ad group ID']:
                                if not criteria_id or criteria_id == k['Keyword ID']:
                                    result['Impressions'] += k['Impressions']
                                    result['Clicks'] += k['Clicks']
                                    result['Cost'] += k['Cost']

        return result


class GoogleAdsBase:
    def __init__(self, directory="./", dump_file_prefix="fooooo", cache=True, account="base"):
        self.google_ads_account = account
        cred_filename = "googleads_brand.yaml"
        self.feed_item_id = 9669007
        if account == "novostroyki-acc":
            cred_filename = "googleads_nov.yaml"
            self.feed_item_id = 170260598
        elif account == "vtorichka-acc":
            cred_filename = "googleads_vtorichka.yaml"
            self.feed_item_id = 245949413
        elif account == "own-acc":
            cred_filename = "googleads_own.yaml"
            self.feed_item_id = 91276129
        elif account == "ipoteka-acc":
            cred_filename = "googleads_ipoteka.yaml"
            self.feed_item_id = 124523402
        elif account == "commerce-acc":
            cred_filename = "googleads_com.yaml"
            self.feed_item_id = 241543429


        self.adwords_client = adwords.AdWordsClient.LoadFromStorage(
            f"{ENVI['CREDENTIALS_DIR']}{cred_filename}"
        )

        # переменные настраивающие кеширование запросов к API
        self.directory = directory
        self.dump_file_prefix = dump_file_prefix
        self.cache = cache

        # переменные устанавливают постраничные запросы к API
        self.PAGE_SIZE = 100
        self.offset = 0

    def cache_enabled(self):
        self.cache = True

    def cache_disabled(self):
        self.cache = False
