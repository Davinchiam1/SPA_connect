from report_loader import Report_loader
from pg_db_connector import update_report_table
import datetime
import time
import json


def load_sales_and_traf(id='',file=False,timedelta=1):

    date = datetime.datetime.now().date() - datetime.timedelta(timedelta)

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



load_sales_and_traf(id='',timedelta=1)

