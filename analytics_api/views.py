from django.shortcuts import render
import clickhouse_connect

import clickhouse_connect
from django.shortcuts import render


def get_client(host, port, username, password, database):
    """
    Returns a ClickHouse client using clickhouse-connect (HTTP protocol).
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,            # usually 8123 for HTTP
        username=username,
        password=password,
        database=database
    )


def advertiser_dashboard(request):
    """
    Retrieves and displays key metrics for a specific advertiser from ClickHouse.
    """
    advertiser_id = 1  # Hardcoded for now, replace with session/user logic

    # Connect to ClickHouse
    # client = get_client(
    #     host='localhost',     # or your remote host/IP
    #     port=8123,
    #     username='revive_user',
    #     password='revive_pass',
    #     database='revive_db'
    # )
    client = get_client(
        host='162.97.141.58',     # or your remote host/IP
        port=8123,
        username='default',
        password='',
        database='re_click_server'
    )
    

    context = {}

    try:
        # 1. Get Overall Summary Data (removed trailing ;)
        summary_query = f"""
            SELECT
                sum(rv_data_summary_ad_hourly.impressions) AS total_impressions,
                sum(rv_data_summary_ad_hourly.clicks) AS total_clicks,
                countDistinct(rv_banners.campaignid) AS total_campaigns,
                countDistinct(rv_banners.bannerid) AS total_ads
            FROM rv_data_summary_ad_hourly
            INNER JOIN rv_banners 
                ON rv_data_summary_ad_hourly.ad_id = rv_banners.bannerid
            INNER JOIN rv_campaigns 
                ON rv_banners.campaignid = rv_campaigns.campaignid
            WHERE rv_campaigns.clientid = {advertiser_id}
        """

        summary_data = client.query(summary_query)

        if summary_data.result_rows:
            total_impressions = int(summary_data.result_rows[0][0] or 0)
            total_clicks = int(summary_data.result_rows[0][1] or 0)

            context['total_impressions'] = total_impressions
            context['total_clicks'] = total_clicks
            context['total_campaigns'] = int(summary_data.result_rows[0][2] or 0)
            context['total_ads'] = int(summary_data.result_rows[0][3] or 0)
            context['ctr'] = round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0

        # 2. Get Time-Series Data (last 30 days) (removed trailing ;)
        daily_query = f"""
            SELECT
                toDate(rv_data_summary_ad_hourly.date_time) AS day,
                sum(rv_data_summary_ad_hourly.impressions) AS daily_impressions,
                sum(rv_data_summary_ad_hourly.clicks) AS daily_clicks
            FROM rv_data_summary_ad_hourly
            INNER JOIN rv_banners 
                ON rv_data_summary_ad_hourly.ad_id = rv_banners.bannerid
            INNER JOIN rv_campaigns 
                ON rv_banners.campaignid = rv_campaigns.campaignid
            WHERE rv_campaigns.clientid = {advertiser_id}
            GROUP BY day
            ORDER BY day ASC
            LIMIT 30
        """

        daily_data = client.query(daily_query)

        # Prepare chart + table data
        context['daily_report'] = []
        context['chart_labels'] = []
        context['chart_impressions'] = []
        context['chart_clicks'] = []

        for row in daily_data.result_rows:
            day, daily_impressions, daily_clicks = row
            day_str = str(day)  # clickhouse-connect returns `datetime.date`

            context['chart_labels'].append(day_str)
            context['chart_impressions'].append(int(daily_impressions))
            context['chart_clicks'].append(int(daily_clicks))

            ctr = round((daily_clicks / daily_impressions) * 100, 2) if daily_impressions > 0 else 0
            context['daily_report'].append({
                'date': day_str,
                'impressions': int(daily_impressions),
                'clicks': int(daily_clicks),
                'ctr': ctr
            })

    except Exception as e:
        print(f"ClickHouse query error: {e}")
        context['error'] = 'Could not retrieve data from the analytics server.'

    return render(request, 'advertiser_dashboard.html', context)
