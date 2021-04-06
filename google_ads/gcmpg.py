from common_constants import constants
from google_ads import gabase
import re
ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/gads_core/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class GCampaigns(gabase.GoogleAdsBase):
    def __init__(self, directory=None, dump_file_prefix="gcmpg", cache=False, account="base"):
        if directory is None:
            directory = f"{ENVI['MAIN_PYSEA_DIR']}alldata/cache"
        super(GCampaigns, self).__init__(directory=directory, dump_file_prefix=dump_file_prefix,
                                         cache=cache, account=account)

        self.data = self.__get_campaigns()
        self.ids_enabled = {i['id'] for i in self.data if i['status'] == 'ENABLED'}

    def __str__(self):
        return f"<<Кампании Google Adwords {len(self.data)} шт.>>"

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    @gabase.dump_to("campaigns")
    @gabase.limit_by(500)
    @gabase.connection_attempts()
    def __get_campaigns(self):
        """
        https://developers.google.com/adwords/api/docs/reference/v201809/CampaignService#get

        :return:
        """
        # Initialize appropriate service.
        service = self.adwords_client.GetService('CampaignService', version='v201809')

        # Construct selector and get all campaigns.
        selector = {
            'fields': ['Id', 'Name', 'Status', 'BudgetId', 'ServingStatus', 'CampaignTrialType'],
            'paging': {
                'startIndex': str(self.offset),
                'numberResults': str(self.PAGE_SIZE)
            }
        }

        page = service.get(selector)

        return page['entries'] if 'entries' in page else {}, int(page['totalNumEntries'])

    def search_by_id(self, campaign_id, ret_field=None):
        for i in self.data:
            if i['id'] == campaign_id:
                if ret_field:
                    return i[ret_field]
                else:
                    return i
        return False

    def search(self, item, ret_field="id"):
        ptr = re.compile(item)
        if ret_field:
            return [i[ret_field] for i in self.data if ptr.search(i['name']) is not None]
        else:
            return [i for i in self.data if ptr.search(i['name']) is not None]

    def search_enabled(self, item, ret_field="id"):
        ptr = re.compile(item)
        if ret_field:
            return [i[ret_field] for i in self.data if ptr.search(i['name']) is not None and i['status'] == 'ENABLED']
        else:
            return [i for i in self.data if ptr.search(i['name']) is not None and i['status'] == 'ENABLED']

    def pop_enabled(self, item):
        ptr = re.compile(item)
        result = []
        for i in self.data[:]:
            if ptr.search(i['name']) is not None and i['status'] == 'ENABLED':
                result.append(i['id'])
                self.data.remove(i)
        return result

    def pop_all(self, item):
        ptr = re.compile(item)
        result = []
        for i in self.data[:]:
            if ptr.search(i['name']) is not None:
                result.append(i['id'])
                self.data.remove(i)
        return result

    def filter(self, key=lambda x: x):
        self.data = list(filter(key, self.data))
        self.ids_enabled = {i['id'] for i in self.data if i['status'] == 'ENABLED'}


class GGroups(gabase.GoogleAdsBase):
    def __init__(self, campaign_ids, directory=None, dump_file_prefix="ggroups", cache=False, account="base"):
        if directory is None:
            directory = f"{ENVI['MAIN_PYSEA_DIR']}alldata/cache"
        super(GGroups, self).__init__(directory=directory, dump_file_prefix=dump_file_prefix,
                                      cache=cache, account=account)

        self.campaign_ids = campaign_ids
        self.data = self.__get_adgroups(campaign_ids)

    def __str__(self):
        return f"<<Группы Google Adwords {len(self.data)} для кампаний {self.campaign_ids}>>"

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __add__(self, other):
        if type(other) is type(self):
            return self.data + other.data
        else:
            return self.data + other

    @gabase.dump_to("groups")
    @gabase.main_array_limit(5)
    @gabase.limit_by(500)
    @gabase.connection_attempts()
    def __get_adgroups(self, campaign_ids):
        """
        https://developers.google.com/adwords/api/docs/reference/v201809/AdGroupService#get

        :return:
        """
        # Initialize appropriate service.
        service = self.adwords_client.GetService('AdGroupService', version='v201809')

        # Construct selector and get all campaigns.
        selector = {
            'fields': ['Id', 'Name', "CampaignId"],
            'predicates': [{
                'field': 'CampaignId',
                'operator': 'IN',
                'values': campaign_ids
            }],
            'paging': {
                'startIndex': str(self.offset),
                'numberResults': str(self.PAGE_SIZE)
            }
        }

        page = service.get(selector)

        return page['entries'] if 'entries' in page else {}, int(page['totalNumEntries'])

    def search(self, item=""):
        if item:
            ptr = re.compile(item)
            return [i['id'] for i in self.data if ptr.search(i['name']) is not None]
        return [i['id'] for i in self.data]


if __name__ == '__main__':
    gc = GCampaigns()
    ids = gc.search_enabled("_msk_brand_cian")
    gg = GGroups(ids)
    print(gg.search())
    print("QKRQ!")
