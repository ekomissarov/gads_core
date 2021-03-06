import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from common_constants import constants
ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/gads_core/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


def qq(client):
    customer_service = client.get_service("CustomerService")

    accessible_customers = customer_service.list_accessible_customers()
    result_total = len(accessible_customers.resource_names)
    print(f"Total results: {result_total}")

    resource_names = accessible_customers.resource_names
    for resource_name in resource_names:
        print(f'Customer resource name: "{resource_name}"')


def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.campaign_budget,
          campaign.serving_status,
          campaign.experiment_type
        FROM campaign
        ORDER BY campaign.id"""

    #['Id', 'Name', 'Status', 'BudgetId', 'ServingStatus', 'CampaignTrialType'],

    # Issues a search request using streaming.
    response = ga_service.search_stream(
        customer_id=customer_id,
        query=query
    )

    for batch in response:
        for row in batch.results:
            if row.campaign.name == "b2c_nn_brand_cian_all_mix_search":
                print(
                    f"ID {row.campaign.id} / "
                    f'"{row.campaign.status.name}" / '
                    f'"{row.campaign.experiment_type.name}" / '
                    f'"{row.campaign.serving_status.name}" / '
                    f'"{row.campaign.campaign_budget}" / '
                    f'"{row.campaign.name}" / '
                )


if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage(path=f"{ENVI['CREDENTIALS_DIR']}legoog_mcc.yaml", version="v8")


    try:
        main(googleads_client, "4080705273")
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'	Error with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)