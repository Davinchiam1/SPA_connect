from report_loader import Report_loader
from pg_db_connector import update_report_table
import datetime
import time
import json


def load_sales_and_traf(id='', file=False, timedelta=2, re_update_rate=1):
    """
        Function for loading last sales and traffic data by GET_SALES_AND_TRAFFIC_REPORT.

        Args:
            id(str): id of already created report, if empty, new report will ber requested. Default empty
            file(bool): mark to load data from previously loaded file (temp.txt in project folder). Default False.
            timedelta(int): counter to set date for creating of a report. reports are created on a date 'timedelta' days
            away from the current one. Defalut 2 days, and can't be less
            re_update_rate(int): countr to set amount of re-updating reports(days). If specified, reload data from
            'timedelta' to 'timedelta+re_update_rate' days from the current one. Default 1

        Returns:
            None

        """

    for i in range(re_update_rate):
        print(i + 1)
        date = datetime.datetime.now().date() - datetime.timedelta(timedelta + i)

        date = date.strftime("%d.%m.%Y")
        salesandtraf = Report_loader()
        if not file:
            report_type = 'GET_SALES_AND_TRAFFIC_REPORT'
            if id == '':
                rep_id = salesandtraf.create_reports(report_type=report_type, start_date=date,
                                                     end_date=date, opinions={"asinGranularity": "SKU"})['reportId']
            else:
                rep_id = id
            time.sleep(20)
            doc_id = salesandtraf.get_report(rep_id)
            while not doc_id:
                time.sleep(20)
                doc_id = salesandtraf.get_report(rep_id)

            doc_content = salesandtraf.transform_report_document(content=salesandtraf.get_report_document(doc_id))

            update_report_table(reports_names=['sales_and_traf', 'sales_and_traf_by_asin'], df_list=doc_content)
        else:
            doc_content = salesandtraf.transform_report_document()

            update_report_table(reports_names=['sales_and_traf', 'sales_and_traf_by_asin'], df_list=doc_content)


load_sales_and_traf(id='', timedelta=2,re_update_rate=30)
