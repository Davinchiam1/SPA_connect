import datetime
import gzip
import json
import pickle
import urllib.parse

import chardet as chardet
import pandas as pd
import io
import requests
from pandas import json_normalize

from credentials import credentials, proxy


def unfold_json(df, col_name, ad_pref=True):
    # df[col_name] = df[col_name].str.replace("'", "\"")
    df = df.fillna(0)

    df[col_name] = df[col_name].apply(lambda x: x[0] if type(x) == list else x)
    df_temp = json_normalize(df[col_name])
    if ad_pref:
        df_temp = df_temp.add_prefix(col_name + '.')
    df = pd.concat([df, df_temp], axis=1)
    return df


class Report_loader:
    """Base class for api requests to SPA"""

    def __init__(self, endpoint="https://sellingpartnerapi-na.amazon.com", marketplace_id="ATVPDKIKX0DER",temp_token='saved token.pkl'):
        self.access_token = None
        self.endpoint = endpoint
        self.marketplace_id = marketplace_id
        self.temp_token=temp_token

    def _autorize(self):

        with open(self.temp_token, "rb") as file:
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

    def get_reports(self, next_token=None, report_type='GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT'):
        """
            Loading 100 resent reports of chosen report_type

            Args:
                 next_token (str): token for next request in recursion, if data divided into several parts, initial None
                 report_type (str): report type according to az types
            Returns:
                df (dataframe): result frame of requested data
        """

        self._autorize()

        if next_token is None:
            request_params = {
                "reportTypes": report_type,
                "pageSize": 100,

            }
        else:
            request_params = {
                "nextToken": next_token
            }

        #     api request
        reports = requests.get(
            self.endpoint + "/reports/2021-06-30/reports" + "?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": self.access_token},
            proxies=proxy).json()

        df = pd.DataFrame(reports['reports'])
        pd.set_option('display.max_columns', None)
        # print(df)
        df.to_excel('reports.xlsx')
        return df

    def get_report(self, report_id=''):
        """
            Get report from service by id.

            Args:
                 report_id (str): extermal id of report
            Returns:
                reportDocumentId (str): id of report main document to download
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
        if report['processingStatus']=='DONE':
            return report['reportDocumentId']
        elif report['processingStatus']=='FATAL' or report['processingStatus']=='CANCELLED':
            raise ValueError("Report progress value malfunction")
        else:
            return None

    def create_reports(self, start_date='01.09.2023', end_date='30.09.2023',
                       report_type='GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT', opinions=None,no_date=False):
        """
           Sending request to create report with chosen parameters.

            Args:
                 start_date (str): start data for creating report
                 end_date (str): end data for creating report
                 report_type (str): report type according to az types
                 opinions (dict): dict with additional request parameters
                 no_date (bool): marker to attach or not attach date values, default False
            Returns:
                report (json): json data of report id
        """

        if opinions is None:
            opinions = {}
        self._autorize()

        start_date = datetime.datetime.strptime(start_date, "%d.%m.%Y")
        end_date = datetime.datetime.strptime(end_date, "%d.%m.%Y")
        if start_date == end_date:
            period = "DAY"
        elif (start_date-end_date).days==-6:
            period = "WEEK"
        else:
            period = "MONTH"
        opinions["reportPeriod"]=period

        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        if no_date:

            request_params = {
                "reportOptions": opinions,
                "reportType": report_type,
                "marketplaceIds": [self.marketplace_id]
            }
        else:
            request_params = {
                "reportOptions": opinions,
                "reportType": report_type,
                "dataStartTime": start_date,
                "dataEndTime": end_date,
                "marketplaceIds": [self.marketplace_id]
            }
        json_data = json.dumps(request_params)

        #     api request
        reports = requests.post(
            self.endpoint + "/reports/2021-06-30/reports", json=json_data,
            headers={"x-amz-access-token": self.access_token, 'Content-Type': 'application/json', 'charset': 'utf-8'},
            proxies=proxy).json()

        print((reports))
        return reports

    def get_report_document(self,
                            report_doc_link='amzn1.spdoc.1.4.na.481c64ee-f907-4f5a-a6ca-c1e14730a78a.T3WGTOODYY4ZH.47700',
                            internal_call=True):
        """
            Loading report document content.

            Args:
                 report_doc_link (str): id of report document
                 internal_call (bool): marker of internal call to concentrate data from recursion data flows
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
        if charset is None:
            with open('temp.txt', 'wb') as file:
                file.write(content)
                return
        else:
            content = content.decode(charset)
        with open('temp.txt', 'w', encoding='utf-8') as file:
            file.write(content)

        if internal_call:
            return content

    def _transform_sales_and_traf(self, content=None):

        """
            Transforming content of report GET_SALES_AND_TRAFFIC_REPORT type to dataframe form.

            Args:
                 content (json): content of report document

            Returns:
                df1 (dataframe): result frame of salesAndTrafficByDate
                df2 (dataframe): result frame of salesAndTrafficByAsin
        """
        data1 = content["salesAndTrafficByDate"]
        data2 = content["salesAndTrafficByAsin"]


        df1 = pd.DataFrame([data1[0]['salesByDate']])


        # Извлекаем значения "amount" из колонок, содержащих внутренний словарь
        df1 = df1.map(lambda x: x.get('amount') if isinstance(x, dict) else x)

        df_temp = pd.json_normalize(data1[0]['trafficByDate'])
        df1 = pd.concat([df1, df_temp], axis=1)

        df1['date']= [data1[0]['date']]
        df1['Currensu']=data1[0]['salesByDate']['orderedProductSales']['currencyCode']

        df2 = pd.DataFrame(data2)
        if 'trafficByAsin' in df2.columns and 'salesByAsin' in df2.columns:
            df2 = unfold_json(df=df2, col_name='trafficByAsin', ad_pref=False)
            df2 = df2.drop('trafficByAsin', axis=1)

            df2 = unfold_json(df=df2, col_name='salesByAsin', ad_pref=False)
            df2 = df2.drop('salesByAsin', axis=1)
            df2['date']=data1[0]['date']
        else:
            df2=pd.DataFrame()
        # df2.to_excel('bisness_report_api 24_10.xlsx')
        # df1.to_excel('ac_report_api 24_10.xlsx')
        return [df1,df2]

    def transform_report_document(self, content=None):
        """
            Transforming content of reports by exact type from report content.

            Args:
                 content (json): content of report document

            Returns:
                df (dataframe): result frame transforming

        """
        if not content:
            with open("temp.txt", "r") as file:
                # Читаем все содержимое файла
                content = file.read()

        data = json.loads(content)
        if data['reportSpecification']['reportType'] == "GET_SALES_AND_TRAFFIC_REPORT":
            return self._transform_sales_and_traf(content=data)
        # if response.headers['content-type'].startswith('text/plain'):
        #     content = content.decode(charset)
        #     with open('temp.txt', 'w', encoding='utf-8') as file:
        #         file.write(content)
        #     if '\n' in content:
        #         lines = content.split('\n')
        #         headers = lines[0].split('\t')
        #         data = [line.split('\t') for line in lines[1:]]
        #         df = pd.DataFrame(data, columns=headers)
        #         df.to_excel(file + '.xlsx', index=False)
        #     else:
        #         with open(file + '.txt', 'w') as file:
        #             file.write(content)


test = Report_loader()
# report_type = 'GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT'
# test.create_reports(report_type=report_type,start_date='12.10.2023',end_date='12.11.2023',opinions={"asinGranularity":"SKU"})
# test.create_reports(report_type=report_type,start_date='05.11.2023',end_date='11.11.2023')
# test.create_reports(report_type=report_type,start_date='01.09.2023', end_date='01.10.2023')
# test.create_reports(report_type=report_type,start_date='24.10.2023',end_date='24.10.2023',no_date=True)
# test.create_reports(report_type=report_type,start_date='01.09.2023', end_date='01.10.2023',opinions={"depersonalized":"true"})
# test.create_reports(report_type=report_type,start_date='01.09.2023', end_date='01.11.2023', opinions={"campaignStartDateFrom": "2023-09-01T00:00:00Z", "campaignStartDateTo": "2023-11-01T00:00:00Z" })
# test.get_reports(report_type=report_type)
# test.transform_report_document()
