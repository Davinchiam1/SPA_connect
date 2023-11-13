import datetime
import gzip
import json
import pickle
import urllib.parse
import pandas as pd
import io
import requests
from pandas import json_normalize

from credentials import credentials, proxy


class SPA_requests:
    """Base class for api requests to SPA"""

    def __init__(self, endpoint="https://sellingpartnerapi-na.amazon.com", marketplace_id="ATVPDKIKX0DER"):
        self.access_token = None
        self.endpoint = endpoint
        self.marketplace_id = marketplace_id

    def _autorize(self):

        with open("saved token.pkl", "rb") as file:
            loaded_data = pickle.load(file)

        saved_token = {'access_token': loaded_data['access_token'], 'time': loaded_data['time']}

        time_difference = (datetime.datetime.now() - saved_token['time']).seconds
        if time_difference > 3500:
            token_response = requests.post(
                "https://api.amazon.com/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": credentials["refresh_token"],
                    "client_id": credentials["lwa_app_id"],
                    "client_secret": credentials["lwa_client_secret"],
                },
                proxies=proxy
            )
            access_token = token_response.json()["access_token"]

            saved_token = {'access_token': access_token, 'time': datetime.datetime.now()}

            with open("saved token.pkl", "wb") as file:
                pickle.dump(saved_token, file)

        self.access_token = saved_token['access_token']

    def last_orders_request(self, last_days=10, next_token=None, initial_call=True):
        """in progress"""
        if initial_call:
            self._autorize()

        request_params = {
            "MarketplaceIds": self.marketplace_id,  # required parameter
            "CreatedAfter": (
                    datetime.datetime.now() - datetime.timedelta(days=last_days)
            ).isoformat(),  # orders created since 30 days ago, the date needs to be in the ISO format
        }

        orders = requests.get(
            self.endpoint + "/orders/v0/orders" + "?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy)

    def fba_inventory(self, next_token=None, initial_call=True):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """
        if initial_call:
            # if it is firs call of func, start from checking authorization data
            self._autorize()
        if next_token is None:
            # request param fo first call
            request_params = {
                "details": "true",
                "granularityType": "Marketplace",
                "granularityId": self.marketplace_id,
                "marketplaceIds": self.marketplace_id
            }
        else:
            # request param fo recursion calls
            request_params = {
                "details": "true",
                "granularityType": "Marketplace",
                "granularityId": self.marketplace_id,
                'nextToken': next_token,
                "marketplaceIds": self.marketplace_id
            }
        #     api request
        inventory = requests.get(
            self.endpoint + "/fba/inventory/v1/summaries" + "?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        df = pd.DataFrame(inventory['payload']['inventorySummaries'])

        if 'pagination' in inventory and inventory['pagination']['nextToken'] != '':
            # recursion part
            print(inventory['pagination']['nextToken'][:5])
            temp_df = self.fba_inventory(next_token=inventory['pagination']['nextToken'], initial_call=False)
            df = pd.concat([df, temp_df], ignore_index=True)

        if initial_call:
            # unfolding of detailed information
            df['inventoryDetails'] = df['inventoryDetails'].astype(str).str.replace("'", "\"")
            df['inventoryDetails'] = df['inventoryDetails'].apply(lambda x: json.loads(x))
            df_temp = json_normalize(df['inventoryDetails'])
            df = pd.concat([df, df_temp], axis=1)
            df = df.drop('inventoryDetails', axis=1)
            # returning result
            df.to_excel('fba_test.xlsx')
        else:
            return df

    def finance_events(self, next_token=None, start_date=datetime.datetime(2023, 9, 1),
                       end_date=datetime.datetime(2023, 10, 1),
                       initial_call=True):
        if initial_call:
            self._autorize()

        if next_token is None:
            request_params = {
                "PostedAfter": start_date.isoformat(),
                "PostedBefore": end_date.isoformat(),
            }
        else:
            request_params = {
                "PostedAfter": start_date.isoformat(),
                "PostedBefore": end_date.isoformat(),
                "NextToken": next_token
            }

        shipments = pd.DataFrame()
        shipments_settle = pd.DataFrame()
        refunds = pd.DataFrame()
        garantee_claim = pd.DataFrame()
        chargeback = pd.DataFrame()

        events_groups = \
            requests.get(
                self.endpoint + "/finances/v0/financialEvents" + "?" + urllib.parse.urlencode(request_params),
                headers={"x-amz-access-token": self.access_token},
                proxies=proxy).json()
        events = events_groups['payload']['FinancialEvents']
        with open('events.json', 'w') as file:
            json.dump(events, file, ensure_ascii=False)
        if 'ShipmentEventList' in events:
            shipments = pd.DataFrame(events['ShipmentEventList'])
        if 'ShipmentSettleEventList' in events:
            shipments_settle = pd.DataFrame(events['ShipmentSettleEventList'])
        if 'RefundEventList' in events:
            refunds = pd.DataFrame(events['RefundEventList'])
        if 'GuaranteeClaimEventList' in events:
            garantee_claim = pd.DataFrame(events['GuaranteeClaimEventList'])
        if 'ChargebackEventList' in events:
            chargeback = pd.DataFrame(events['ChargebackEventList'])

        if 'NextToken' in events_groups['payload'] and events_groups['payload']['NextToken'] is not None:

            temp_events = self.finance_events(next_token=events_groups['payload']['NextToken'], start_date=start_date,
                                              end_date=end_date, initial_call=False)
            if 'ShipmentEventList' in events:
                shipments = pd.concat([shipments, temp_events['ShipmentEventList']])
            if 'ShipmentSettleEventList' in events:
                shipments_settle = pd.concat([shipments_settle, temp_events['ShipmentEventList']])
            if 'RefundEventList' in events:
                refunds = pd.concat([refunds, temp_events['RefundEventList']])
            if 'GuaranteeClaimEventList' in events:
                garantee_claim = pd.concat([garantee_claim, temp_events['GuaranteeClaimEventList']])
            if 'ChargebackEventList' in events:
                chargeback = pd.concat([chargeback, temp_events['ChargebackEventList']])

        if initial_call:

            # Сохраняем каждый DataFrame на отдельном листе
            shipments.to_excel('Shipments.xlsx', index=False)
            shipments_settle.to_excel('Shipments_settle.xlsx', index=False)
            refunds.to_excel('Refunds.xlsx', index=False)
            garantee_claim.to_excel('Garantee_claim.xlsx', index=False)
            chargeback.to_excel('Chargeback.xlsx', index=False)
        else:
            temp_dict = {}
            if 'ShipmentEventList' in events:
                temp_dict['ShipmentEventList'] = shipments
            if 'ShipmentSettleEventList' in events:
                temp_dict['ShipmentSettleEventList'] = shipments_settle
            if 'RefundEventList' in events:
                temp_dict['RefundEventList'] = refunds
            if 'GuaranteeClaimEventList' in events:
                temp_dict['GuaranteeClaimEventList'] = garantee_claim
            if 'ChargebackEventList' in events:
                temp_dict['ChargebackEventList'] = chargeback

            return temp_dict

    def finance_events_groups(self, next_token=None, start_date=datetime.datetime(2023, 9, 1),
                              end_date=datetime.datetime(2023, 10, 1), initial_call=True):
        if initial_call:
            self._autorize()

        if next_token is None:
            request_params = {
                "FinancialEventGroupStartedBefore": end_date.isoformat(),
                "FinancialEventGroupStartedAfter": start_date.isoformat(),
            }
        else:
            request_params = {
                "FinancialEventGroupStartedBefore": end_date.isoformat(),
                "FinancialEventGroupStartedAfter": start_date.isoformat(),
                "NextToken": next_token
            }

        events_groups = \
            requests.get(
                self.endpoint + "/finances/v0/financialEventGroups" + "?" + urllib.parse.urlencode(request_params),
                headers={"x-amz-access-token": self.access_token},
                proxies=proxy).json()
        events_groups = events_groups['payload']
        df = pd.DataFrame(events_groups['FinancialEventGroupList'])
        if 'NextToken' in events_groups and events_groups['NextToken'] is not None:
            print(1)
            temp_df = self.finance_events(next_token=events_groups['NextToken'], start_date=start_date,
                                          end_date=end_date, initial_call=False)
            df = pd.concat([df, temp_df])

        if initial_call:
            df.to_excel('test.xlsx')
        else:
            return df

    def get_reports_schedules(self, next_token=None, report_type='GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT', initial_call=True):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """

        self._autorize()


        request_params = {
            "reportTypes": report_type,
        }

        #     api request
        reports = requests.get(
            self.endpoint + "/reports/2021-06-30/schedules" + "?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        df = pd.DataFrame(reports['reports'])
        pd.set_option('display.max_columns', None)
        print(df)
        df.to_excel('reports_schedules.xlsx')

    def get_report(self, report_id=''):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """

        self._autorize()



        #     api request
        report = requests.get(
            self.endpoint + "/reports/2021-06-30/reports" + "/" + report_id,
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        print(report)
        with open('report.txt', 'w') as file:
            file.write(str(report))

    def create_reports(self, start_date=datetime.datetime(2023, 9, 1), end_date=datetime.datetime(2023, 9, 30),
                       report_type='GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT'):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """

        self._autorize()

        request_params = {
            "reportOptions": {"reportPeriod": "MONTH"},
            "reportType": report_type,
            "dataStartTime": "2023-09-01T00:00:00.000Z",
            "dataEndTime": "2023-09-30T00:00:00.000Z",
            "marketplaceIds": [self.marketplace_id]
        }
        json_data = json.dumps(request_params)

        #     api request
        reports = requests.post(
            self.endpoint + "/reports/2021-06-30/reports", json=json_data,
            headers={"x-amz-access-token": self.access_token, 'Content-Type': 'application/json', 'charset': 'utf-8'},
            proxies=proxy).json()

        print((reports))

    def get_report_document(self, json_marker='', file='test',
                            report_doc_link='amzn1.spdoc.1.4.na.481c64ee-f907-4f5a-a6ca-c1e14730a78a.T3WGTOODYY4ZH.47700', ):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """

        self._autorize()

        #     api request
        doc = requests.get(
            self.endpoint + "/reports/2021-06-30/documents/" + report_doc_link,
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        print(doc)
        response = requests.get(doc['url'], stream=True, proxies=proxy)
        if 'compressionAlgorithm' in doc and doc['compressionAlgorithm'] == "GZIP":
            content = gzip.GzipFile(fileobj=io.BytesIO(response.content)).read()
        else:
            content = response.content
        charset = response.encoding
        content1 = content.decode(charset)
        with open('temp.txt', 'w',encoding='utf-8') as file:
            file.write(content1)
        if response.headers['content-type'].startswith('text/plain'):
            content = content.decode(charset)
            with open('temp.txt', 'w',encoding='utf-8') as file:
                file.write(content)
            if '\n' in content:
                lines = content.split('\n')
                headers = lines[0].split('\t')
                data = [line.split('\t') for line in lines[1:]]
                df = pd.DataFrame(data, columns=headers)
                df.to_excel(file + '.xlsx', index=False)
            else:
                with open(file + '.txt', 'w') as file:
                    file.write(content)

            # Сохраняем датафрейм в эксель-файл


        else:
            # Handle content with binary data/other media types here.
            with open('output_test.txt', 'wb') as file:
                file.write(content)

    def sales_metrics(self, start_date=datetime.datetime(2023, 9, 1), end_date=datetime.datetime(2023, 10, 1),
                      initial_call=True):
        """
            Loading fba inventory data for marketplase.

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 initial_call (bool): marker of initial call to concentrate data from recursion data flows
            Returns:
                df (dataframe): result frame of requested data
        """
        if initial_call:
            # if it is firs call of func, start from checking authorization data
            self._autorize()
        formatted_start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        formatted_end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        # Объединение форматированных дат в одну строку
        date_range_string = f"{formatted_start_date}-07:00--{formatted_end_date}-07:00"

        # request param fo first call
        request_params = {
            "marketplaceIds": self.marketplace_id,
            "interval": date_range_string,
            "granularity": 'Day',

        }

        #     api request
        sales = requests.get(
            self.endpoint + "/sales/v1/orderMetrics" + "?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        df = pd.DataFrame(sales['payload'][0])

        if initial_call:
            # unfolding of detailed information
            # df['inventoryDetails'] = df['inventoryDetails'].astype(str).str.replace("'", "\"")
            # df['inventoryDetails'] = df['inventoryDetails'].apply(lambda x: json.loads(x))
            # df_temp = json_normalize(df['inventoryDetails'])
            # df = pd.concat([df, df_temp], axis=1)
            # df = df.drop('inventoryDetails', axis=1)
            # # returning result
            df.to_excel('fba_test.xlsx')


test = SPA_requests()
test.finance_events()
# test.fba_inventory()
# test.sales_metrics()
report_type='GET_SALES_AND_TRAFFIC_REPORT'
# test.create_reports(report_type=report_type)
# test.get_reports(report_type=report_type)
# test.get_report('86943019660')
# test.get_report_document(report_doc_link='amzn1.spdoc.1.4.na.5cb8ae01-5ab7-412d-a704-fd3b6fad074a.T3JS88H1U1CUOK.2900', file='BRAND_ANALYTICS', json_marker='salesAndTrafficByDate')
# test.get_reports_schedules(report_type=report_type)