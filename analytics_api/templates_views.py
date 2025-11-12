# from django.shortcuts import render
# import clickhouse_connect

# import clickhouse_connect
# from django.shortcuts import render


# def get_client(host, port, username, password, database):
#     """
#     Returns a ClickHouse client using clickhouse-connect (HTTP protocol).
#     """
#     return clickhouse_connect.get_client(
#         host=host,
#         port=port,            # usually 8123 for HTTP
#         username=username,
#         password=password,
#         database=database
#     )


# def advertiser_dashboard(request):
#     """
#     Retrieves and displays key metrics for a specific advertiser from ClickHouse.
#     """
#     advertiser_id = 1  # Hardcoded for now, replace with session/user logic

#     # Connect to ClickHouse
#     # client = get_client(
#     #     host='localhost',     # or your remote host/IP
#     #     port=8123,
#     #     username='revive_user',
#     #     password='revive_pass',
#     #     database='revive_db'
#     # )
#     client = get_client(
#         host='localhost',     # or your remote host/IP
#         port=8123,
#         username='revive_user',
#         password='revive_pass',
#         database='revive_db'
#     )
    

#     context = {}

#     try:
#         # 1. Get Overall Summary Data (removed trailing ;)
#         summary_query = f"""
#             SELECT
#                 sum(rv_data_summary_ad_hourly.impressions) AS total_impressions,
#                 sum(rv_data_summary_ad_hourly.clicks) AS total_clicks,
#                 countDistinct(rv_banners.campaignid) AS total_campaigns,
#                 countDistinct(rv_banners.bannerid) AS total_ads
#             FROM rv_data_summary_ad_hourly
#             INNER JOIN rv_banners 
#                 ON rv_data_summary_ad_hourly.ad_id = rv_banners.bannerid
#             INNER JOIN rv_campaigns 
#                 ON rv_banners.campaignid = rv_campaigns.campaignid
#             WHERE rv_campaigns.clientid = {advertiser_id}
#         """

#         summary_data = client.query(summary_query)

#         if summary_data.result_rows:
#             total_impressions = int(summary_data.result_rows[0][0] or 0)
#             total_clicks = int(summary_data.result_rows[0][1] or 0)

#             context['total_impressions'] = total_impressions
#             context['total_clicks'] = total_clicks
#             context['total_campaigns'] = int(summary_data.result_rows[0][2] or 0)
#             context['total_ads'] = int(summary_data.result_rows[0][3] or 0)
#             context['ctr'] = round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0

#         # 2. Get Time-Series Data (last 30 days) (removed trailing ;)
#         daily_query = f"""
#             SELECT
#                 toDate(rv_data_summary_ad_hourly.date_time) AS day,
#                 sum(rv_data_summary_ad_hourly.impressions) AS daily_impressions,
#                 sum(rv_data_summary_ad_hourly.clicks) AS daily_clicks
#             FROM rv_data_summary_ad_hourly
#             INNER JOIN rv_banners 
#                 ON rv_data_summary_ad_hourly.ad_id = rv_banners.bannerid
#             INNER JOIN rv_campaigns 
#                 ON rv_banners.campaignid = rv_campaigns.campaignid
#             WHERE rv_campaigns.clientid = {advertiser_id}
#             GROUP BY day
#             ORDER BY day ASC
#             LIMIT 30
#         """

#         daily_data = client.query(daily_query)

#         # Prepare chart + table data
#         context['daily_report'] = []
#         context['chart_labels'] = []
#         context['chart_impressions'] = []
#         context['chart_clicks'] = []

#         for row in daily_data.result_rows:
#             day, daily_impressions, daily_clicks = row
#             day_str = str(day)  # clickhouse-connect returns `datetime.date`

#             context['chart_labels'].append(day_str)
#             context['chart_impressions'].append(int(daily_impressions))
#             context['chart_clicks'].append(int(daily_clicks))

#             ctr = round((daily_clicks / daily_impressions) * 100, 2) if daily_impressions > 0 else 0
#             context['daily_report'].append({
#                 'date': day_str,
#                 'impressions': int(daily_impressions),
#                 'clicks': int(daily_clicks),
#                 'ctr': ctr
#             })

#     except Exception as e:
#         print(f"ClickHouse query error: {e}")
#         context['error'] = 'Could not retrieve data from the analytics server.'

#     return render(request, 'advertiser_dashboard.html', context)


from django.shortcuts import render
import clickhouse_connect
import pymysql
from datetime import datetime

def get_ch_client():
    # return clickhouse_connect.get_client(
    #     host='localhost',
    #     port=8123,
    #     username='revive_user',
    #     password='revive_pass',
    #     database='revive_db'
    # )
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="re_click_server"
    )
    
    
def get_mysql_conn():
    return pymysql.connect(
        host='localhost',
        user='revive_user',
        password='revive_pass',
        database='revive_db',
        cursorclass=pymysql.cursors.DictCursor
    )

# def admin_dashboard(request):
#     client = get_ch_client()
#     mysql_conn = get_mysql_conn()
#     mysql_cursor = mysql_conn.cursor()
#     context = {}

#     try:
#         # 1. Overall Summary
#         summary_query = """
#             SELECT
#                 sum(T1.impressions) AS total_impressions,
#                 sum(T1.clicks) AS total_clicks,
#                 countDistinct(T2.campaignid) AS total_campaigns,
#                 countDistinct(T2.bannerid) AS total_ads,
#                 countDistinct(T3.clientid) AS total_advertisers
#             FROM rv_data_summary_ad_hourly AS T1
#             INNER JOIN rv_banners AS T2 ON T1.ad_id = T2.bannerid
#             INNER JOIN rv_campaigns AS T3 ON T2.campaignid = T3.campaignid
#         """
#         summary_data = client.query(summary_query).result_rows
#         if summary_data:
#             total_impressions, total_clicks, total_campaigns, total_ads, total_advertisers = summary_data[0]
#             context['total_impressions'] = int(total_impressions or 0)
#             context['total_clicks'] = int(total_clicks or 0)
#             context['total_campaigns'] = int(total_campaigns or 0)
#             context['total_ads'] = int(total_ads or 0)
#             context['total_advertisers'] = int(total_advertisers or 0)
#             context['ctr'] = round((context['total_clicks'] / context['total_impressions']) * 100, 2) if context['total_impressions'] > 0 else 0

#         # 2. Daily Time Series (last 30 days)
#         daily_query = """
#             SELECT
#                 toDate(T1.date_time) AS day,
#                 sum(T1.impressions) AS daily_impressions,
#                 sum(T1.clicks) AS daily_clicks
#             FROM rv_data_summary_ad_hourly AS T1
#             GROUP BY day
#             HAVING day >= today() - interval 30 day
#             ORDER BY day ASC
#         """
#         daily_data = client.query(daily_query).result_rows
#         context['daily_report'] = []
#         context['chart_labels'] = []
#         context['chart_impressions'] = []
#         context['chart_clicks'] = []

#         for row in daily_data:
#             day, daily_impressions, daily_clicks = row
#             day_str = str(day)
#             ctr = round((daily_clicks / daily_impressions) * 100, 2) if daily_impressions > 0 else 0
#             context['daily_report'].append({
#                 'date': day_str,
#                 'impressions': int(daily_impressions),
#                 'clicks': int(daily_clicks),
#                 'ctr': ctr
#             })
#             context['chart_labels'].append(day_str)
#             context['chart_impressions'].append(int(daily_impressions))
#             context['chart_clicks'].append(int(daily_clicks))

#         # 3. Top 10 Banners
#         banners_query = """
#             SELECT
#                 T2.bannerid,
#                 sum(T1.impressions) AS total_impressions,
#                 sum(T1.clicks) AS total_clicks
#             FROM rv_data_summary_ad_hourly AS T1
#             INNER JOIN rv_banners AS T2 ON T1.ad_id = T2.bannerid
#             GROUP BY T2.bannerid
#             ORDER BY total_impressions DESC
#             LIMIT 10
#         """
#         banners_data = client.query(banners_query).result_rows
#         context['banners'] = []
#         for banner_id, impressions, clicks in banners_data:
#             mysql_cursor.execute("SELECT storagetype FROM rv_banners WHERE bannerid=%s", (banner_id,))
#             banner = mysql_cursor.fetchone()
#             banner_type = banner['storagetype'] if banner else 'N/A'
#             ctr = round((clicks / impressions) * 100, 2) if impressions > 0 else 0
#             context['banners'].append({
#                 'id': banner_id,
#                 'type': banner_type,
#                 'impressions': int(impressions),
#                 'clicks': int(clicks),
#                 'ctr': ctr
#             })

#         # 4. Top 10 Zones
#         zones_query = """
#             SELECT
#                 T1.zone_id,
#                 sum(T1.actual_impressions) AS total_impressions
#             FROM rv_data_summary_zone_impression_history AS T1
#             GROUP BY T1.zone_id
#             ORDER BY total_impressions DESC
#             LIMIT 10
#         """
#         zones_data = client.query(zones_query).result_rows
#         context['zones'] = []
#         for zone_id, impressions in zones_data:
#             mysql_cursor.execute("SELECT zonename FROM rv_zones WHERE zoneid=%s", (zone_id,))
#             zone = mysql_cursor.fetchone()
#             zone_name = zone['zonename'] if zone else 'Unknown'
#             context['zones'].append({
#                 'id': zone_id,
#                 'name': zone_name,
#                 'impressions': int(impressions)
#             })

#         # 5. Top 10 Advertisers
#         advertisers_query = """
#             SELECT
#                 T3.clientid,
#                 sum(T1.impressions) AS total_impressions,
#                 sum(T1.clicks) AS total_clicks
#             FROM rv_data_summary_ad_hourly AS T1
#             INNER JOIN rv_banners AS T2 ON T1.ad_id = T2.bannerid
#             INNER JOIN rv_campaigns AS T3 ON T2.campaignid = T3.campaignid
#             GROUP BY T3.clientid
#             ORDER BY total_impressions DESC
#             LIMIT 10
#         """
#         advertisers_data = client.query(advertisers_query).result_rows
#         context['advertisers'] = []
#         for advertiser_id, impressions, clicks in advertisers_data:
#             mysql_cursor.execute("SELECT clientname FROM rv_clients WHERE clientid=%s", (advertiser_id,))
#             adv = mysql_cursor.fetchone()
#             adv_name = adv['clientname'] if adv else 'Unknown'
#             ctr = round((clicks / impressions) * 100, 2) if impressions > 0 else 0
#             context['advertisers'].append({
#                 'id': advertiser_id,
#                 'name': adv_name,
#                 'impressions': int(impressions),
#                 'clicks': int(clicks),
#                 'ctr': ctr
#             })

#     except Exception as e:
#         print(f"Dashboard query error: {e}")
#         context['error'] = 'Could not retrieve data from analytics server.'
#     finally:
#         mysql_conn.close()

#     return render(request, 'admin_dashboard.html', context)

from django.shortcuts import render
import clickhouse_connect
from datetime import datetime

def get_client(host, port, username, password, database):
    """
    Returns a ClickHouse client.
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )
    
    
    
    
    
# views.py
from django.shortcuts import render
# from .clickhouse_client import get_client  # assuming you placed get_client in clickhouse_client.py

def admin_dashboard(request):
    client = get_client()

    query = """
        SELECT 
            (SELECT COUNT(*) FROM rv_clients) AS total_advertisers,
            (SELECT COUNT(*) FROM rv_campaigns) AS total_campaigns,
            (SELECT COUNT(*) FROM rv_banners) AS total_banners,
            (SELECT COUNT(*) FROM rv_zones) AS total_zones,
            COALESCE((SELECT SUM(impressions) FROM rv_data_summary_ad_hourly), 0) AS total_impressions,
            COALESCE((SELECT SUM(clicks) FROM rv_data_summary_ad_hourly), 0) AS total_clicks
    """

    result = client.query(query).result_rows[0]

    total_impressions = int(result[4])
    total_clicks = int(result[5])
    ctr = round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0

    metrics = [
        ("Advertisers", result[0]),
        ("Campaigns", result[1]),
        ("Banners", result[2]),
        ("Zones", result[3]),
        ("Impressions", total_impressions),
        ("Clicks", total_clicks),
        ("CTR (%)", ctr)
    ]

    return render(request, "admin_dashboard2.html", {
        "metrics": metrics,
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "ctr": ctr
    })


def all_zones(request):
    """
    Retrieves and displays a detailed list of all zones.
    """
    client = get_client()
    
    context = {}

    try:
        # Query to get all zones with their total impressions
        zones_query = """
            SELECT
                T2.zoneid,
                T2.zonename,
                T2.description,
                T2.width,
                T2.height,
                sum(T1.actual_impressions) AS total_impressions
            FROM rv_zones AS T2
            LEFT JOIN rv_data_summary_zone_impression_history AS T1 ON T1.zone_id = T2.zoneid
            GROUP BY T2.zoneid, T2.zonename, T2.description, T2.width, T2.height
            ORDER BY T2.zoneid ASC
        """
        
        zones_data = client.query(zones_query).result_rows
        
        context['zones'] = []
        for row in zones_data:
            zone_id, zonename, description, width, height, impressions = row
            # Use 'or 0' to handle NULL impressions from the LEFT JOIN
            context['zones'].append({
                'id': zone_id,
                'name': zonename,
                'description': description,
                'width': width,
                'height': height,
                'impressions': int(impressions or 0)
            })

    except Exception as e:
        print(f"ClickHouse query error: {e}")
        context['error'] = 'Could not retrieve data from the analytics server.'
    
    return render(request, 'all_zones.html', context)


def advertiser_campaigns_view(request):
    # 🔹 Connect to your ClickHouse server
    client = clickhouse_connect.get_client(
        host="localhost",    # or "127.0.0.1"
        port=8123,           # HTTP port exposed by Docker
        username="revive_user", 
        password="revive_pass", 
        database="revive_db"
    )

    # 🔹 Query advertisers (clients) and campaigns
    query = """
        SELECT 
            T1.clientid AS advertiser_id,
            T1.clientname AS advertiser_name,
            COUNT(T2.campaignid) AS total_campaigns
        FROM rv_clients AS T1
        LEFT JOIN rv_campaigns AS T2 
            ON T2.clientid = T1.clientid
        GROUP BY T1.clientid, T1.clientname
        ORDER BY T1.clientid ASC
    """

    result = client.query(query)

    # Convert rows into a list of dicts
    advertisers = [
        {
            "advertiser_id": row[0],
            "advertiser_name": row[1],
            "total_campaigns": row[2],
        }
        for row in result.result_rows
    ]

    # Pass to template
    return render(request, "all_advertisers.html", {"advertisers": advertisers})



from django.shortcuts import render
import clickhouse_connect
import pymysql

def get_ch_client():
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='revive_user',
        password='revive_pass',
        database='revive_db'
    )

def get_mysql_conn():
    return pymysql.connect(
        host='localhost',
        user='revive_user',
        password='revive_pass',
        database='revive_db',
        cursorclass=pymysql.cursors.DictCursor
    )

# def advertiser_dashboard(request, client_id):
#     client = get_ch_client()
#     mysql_conn = get_mysql_conn()
#     mysql_cursor = mysql_conn.cursor()
#     context = {'client_id': client_id}

#     try:
#         # 1. Summary: Impressions & Clicks for this advertiser
#         summary_query = f"""
#             SELECT
#                 SUM(T1.impressions) AS total_impressions,
#                 SUM(T1.clicks) AS total_clicks
#             FROM rv_data_summary_ad_hourly AS T1
#             INNER JOIN rv_banners AS T2 ON T1.ad_id = T2.bannerid
#             INNER JOIN rv_campaigns AS T3 ON T2.campaignid = T3.campaignid
#             WHERE T3.clientid = {client_id}
#         """
#         summary_data = client.query(summary_query).result_rows
#         total_impressions, total_clicks = summary_data[0] if summary_data else (0, 0)
#         context['total_impressions'] = int(total_impressions or 0)
#         context['total_clicks'] = int(total_clicks or 0)
#         context['ctr'] = round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0

#         # 2. Campaigns & Banners counts
#         mysql_cursor.execute("SELECT COUNT(DISTINCT campaignid) AS total_campaigns FROM rv_campaigns WHERE clientid=%s", (client_id,))
#         context['total_campaigns'] = mysql_cursor.fetchone()['total_campaigns']

#         mysql_cursor.execute("SELECT COUNT(DISTINCT bannerid) AS total_banners FROM rv_banners WHERE campaignid IN (SELECT campaignid FROM rv_campaigns WHERE clientid=%s)", (client_id,))
#         context['total_banners'] = mysql_cursor.fetchone()['total_banners']

#         # 3. Top 10 Banners by Impressions
#         banners_query = f"""
#             SELECT
#                 T2.bannerid,
#                 SUM(T1.impressions) AS impressions,
#                 SUM(T1.clicks) AS clicks
#             FROM rv_data_summary_ad_hourly AS T1
#             INNER JOIN rv_banners AS T2 ON T1.ad_id = T2.bannerid
#             INNER JOIN rv_campaigns AS T3 ON T2.campaignid = T3.campaignid
#             WHERE T3.clientid = {client_id}
#             GROUP BY T2.bannerid
#             ORDER BY impressions DESC
#             LIMIT 10
#         """
#         banners_data = client.query(banners_query).result_rows
#         context['banners'] = []
#         for banner_id, impressions, clicks in banners_data:
#             mysql_cursor.execute("SELECT storagetype FROM rv_banners WHERE bannerid=%s", (banner_id,))
#             banner = mysql_cursor.fetchone()
#             banner_type = banner['storagetype'] if banner else 'N/A'
#             ctr = round((clicks / impressions) * 100, 2) if impressions > 0 else 0
#             context['banners'].append({
#                 'id': banner_id,
#                 'type': banner_type,
#                 'impressions': int(impressions),
#                 'clicks': int(clicks),
#                 'ctr': ctr
#             })

#         # 4. Top 10 Zones for this advertiser
#         zones_query = f"""
#             SELECT 
#                 z.zoneid,
#                 z.zonename,
#                 SUM(h.actual_impressions) AS impressions,
#                 SUM(h.clicks) AS clicks
#             FROM rv_data_summary_zone_impression_history h
#             JOIN rv_zones z ON h.zone_id = z.zoneid
#             JOIN rv_ad_zone_assoc aza ON z.zoneid = aza.zone_id
#             JOIN rv_banners b ON aza.ad_id = b.bannerid
#             JOIN rv_campaigns c ON b.campaignid = c.campaignid
#             WHERE c.clientid = {advertiser_id}
#             GROUP BY z.zoneid, z.zonename
#             ORDER BY impressions DESC
#             LIMIT 10;


#         """
#         zones_data = client.query(zones_query).result_rows
#         context['zones'] = []
#         for zone_id, impressions in zones_data:
#             mysql_cursor.execute("SELECT zonename FROM rv_zones WHERE zoneid=%s", (zone_id,))
#             zone = mysql_cursor.fetchone()
#             zone_name = zone['zonename'] if zone else 'Unknown'
#             context['zones'].append({
#                 'id': zone_id,
#                 'name': zone_name,
#                 'impressions': int(impressions)
#             })

#     except Exception as e:
#         print(f"Advertiser dashboard error: {e}")
#         context['error'] = "Could not fetch advertiser data."
#     finally:
#         mysql_conn.close()

#     return render(request, 'advertiser_dashboard.html', context)




# All publishers
from django.shortcuts import render
import clickhouse_connect

def get_client():
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="revive_user",
        password="revive_pass",
        database="revive_db"
    )

def publisher_list(request):
    client = get_client()

    query = """
    SELECT 
        aff.affiliateid AS publisher_id,
        aff.name AS publisher_name,
        aff.contact AS contact_name,
        aff.email AS contact_email,
        COUNT(z.zoneid) AS total_zones
    FROM rv_affiliates AS aff
    LEFT JOIN rv_zones AS z 
        ON aff.affiliateid = z.affiliateid
    GROUP BY aff.affiliateid, aff.name, aff.contact, aff.email
    ORDER BY aff.affiliateid ASC
    """

    result = client.query(query)
    columns = result.column_names
    rows = result.result_rows

    publishers = [dict(zip(columns, row)) for row in rows]

    return render(request, "publisher_list.html", {"publishers": publishers})



# def publisher_dashboard(request, publisher_id):
#     return render(request, 'index.html')



# publisher dashboard



from django.shortcuts import render
import clickhouse_connect
from pypika import Query, Table, functions as fn

def get_client():
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="revive_user",
        password="revive_pass",
        database="revive_db"
    )

def publisher_dashboard(request, publisher_id):
    client = get_client()

    # --- 1. Publisher Info ---
    query_info = f"""
        SELECT 
            p.account_id AS publisher_id,
            p.account_name AS name,
            COUNT(DISTINCT z.zoneid) AS total_zones,
            COUNT(DISTINCT b.bannerid) AS total_banners,
            COALESCE(SUM(s.impressions), 0) AS total_impressions,
            COALESCE(SUM(s.clicks), 0) AS total_clicks
        FROM revive_db.rv_accounts AS p
        LEFT JOIN revive_db.rv_zones AS z ON p.account_id = z.affiliateid
        LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.zone_id = z.zoneid
        LEFT JOIN revive_db.rv_banners AS b ON b.bannerid = s.ad_id
        LEFT JOIN revive_db.rv_campaigns AS c ON c.campaignid = b.campaignid
        LEFT JOIN revive_db.rv_clients AS cl ON cl.clientid = c.clientid
        WHERE p.account_id = {publisher_id}
        GROUP BY p.account_id, p.account_name
    """

    result_info = client.query(query_info).result_rows

    publisher = None
    if result_info:
        row = result_info[0]
        total_impressions = int(row[4] or 0)
        total_clicks = int(row[5] or 0)
        publisher = {
            "publisher_id": row[0],
            "name": row[1],
            "total_zones": row[2],
            "total_banners": row[3],
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0
        }

    # --- 2. Zone-wise Stats ---
    query_zones = f"""
        SELECT 
            z.zoneid AS zone_id,
            z.zonename AS zone_name,
            COUNT(DISTINCT b.bannerid) AS total_banners,
            COALESCE(SUM(s.impressions), 0) AS total_impressions,
            COALESCE(SUM(s.clicks), 0) AS total_clicks
        FROM revive_db.rv_zones AS z
        LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.zone_id = z.zoneid
        LEFT JOIN revive_db.rv_banners AS b ON b.bannerid = s.ad_id
        LEFT JOIN revive_db.rv_campaigns AS c ON c.campaignid = b.campaignid
        LEFT JOIN revive_db.rv_clients AS cl ON cl.clientid = c.clientid
        WHERE z.affiliateid = {publisher_id}
        GROUP BY z.zoneid, z.zonename
        ORDER BY total_impressions DESC
    """

    zone_result = client.query(query_zones).result_rows

    zones = []
    chart_labels = []
    chart_impressions = []
    chart_clicks = []

    for row in zone_result:
        zone_id, zone_name, total_banners, total_impressions, total_clicks = row
        total_impressions = int(total_impressions or 0)
        total_clicks = int(total_clicks or 0)
        ctr = round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0
        zones.append({
            "zone_id": zone_id,
            "zone_name": zone_name,
            "total_banners": total_banners,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": ctr
        })
        chart_labels.append(zone_name)
        chart_impressions.append(total_impressions)
        chart_clicks.append(total_clicks)

    return render(request, "publisher_dashboard.html", {
        "publisher": publisher,
        "zones": zones,
        "chart_labels": chart_labels,
        "chart_impressions": chart_impressions,
        "chart_clicks": chart_clicks
    })




def zone_stats(request, zone_id):
    client = get_client()

    query = f"""
        SELECT 
            z.zoneid AS zone_id,
            z.zonename AS zone_name,
            SUM(m.count) AS total_impressions,
            SUM(c.count) AS total_clicks
        FROM rv_zones AS z
        LEFT JOIN rv_ad_zone_assoc AS az ON z.zoneid = az.zone_id
        LEFT JOIN rv_banners AS b ON az.ad_id = b.bannerid
        LEFT JOIN rv_data_bkt_m AS m ON b.bannerid = m.creative_id
        LEFT JOIN rv_data_bkt_c AS c ON b.bannerid = c.creative_id
        WHERE z.zoneid = {zone_id}
        GROUP BY z.zoneid, z.zonename
    """
    result = client.query(query).result_rows

    zone = None
    if result:
        zone_id, zone_name, total_impressions, total_clicks = result[0]
        zone = {
            "zone_id": zone_id,
            "zone_name": zone_name,
            "total_impressions": total_impressions or 0,
            "total_clicks": total_clicks or 0,
            "ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0
        }

    return render(request, "zone_stats.html", {"zone": zone})



def advertiser_dashboard(request, advertiser_id):
    client = get_client()

    # --- 1. Advertiser Overview ---
    query_info = f"""
        SELECT 
            cl.clientid AS advertiser_id,
            cl.clientname AS name,
            COUNT(DISTINCT c.campaignid) AS total_campaigns,
            COUNT(DISTINCT b.bannerid) AS total_banners,
            COUNT(DISTINCT s.zone_id) AS total_zones,
            COALESCE(SUM(s.impressions), 0) AS total_impressions,
            COALESCE(SUM(s.clicks), 0) AS total_clicks
        FROM revive_db.rv_clients AS cl
        LEFT JOIN revive_db.rv_campaigns AS c ON cl.clientid = c.clientid
        LEFT JOIN revive_db.rv_banners AS b ON b.campaignid = c.campaignid
        LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.ad_id = b.bannerid
        WHERE cl.clientid = {advertiser_id}
        GROUP BY cl.clientid, cl.clientname
    """

    result_info = client.query(query_info).result_rows

    advertiser = None
    if result_info:
        row = result_info[0]
        total_impressions = int(row[5] or 0)
        total_clicks = int(row[6] or 0)
        advertiser = {
            "advertiser_id": row[0],
            "name": row[1],
            "total_campaigns": row[2],
            "total_banners": row[3],
            "total_zones": row[4],
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0
        }

    # --- 2. Zone-Level Breakdown ---
    query_zones = f"""
        SELECT 
            z.zoneid AS zone_id,
            z.zonename AS zone_name,
            COUNT(DISTINCT b.bannerid) AS total_banners,
            COALESCE(SUM(s.impressions), 0) AS total_impressions,
            COALESCE(SUM(s.clicks), 0) AS total_clicks
        FROM revive_db.rv_data_summary_ad_hourly AS s
        JOIN revive_db.rv_zones AS z ON z.zoneid = s.zone_id
        JOIN revive_db.rv_banners AS b ON b.bannerid = s.ad_id
        JOIN revive_db.rv_campaigns AS c ON c.campaignid = b.campaignid
        JOIN revive_db.rv_clients AS cl ON cl.clientid = c.clientid
        WHERE cl.clientid = {advertiser_id}
        GROUP BY z.zoneid, z.zonename
        ORDER BY total_impressions DESC
    """

    zone_result = client.query(query_zones).result_rows

    zones = []
    chart_labels = []
    chart_impressions = []
    chart_clicks = []

    for row in zone_result:
        zone_id, zone_name, total_banners, total_impressions, total_clicks = row
        total_impressions = int(total_impressions or 0)
        total_clicks = int(total_clicks or 0)
        ctr = round((total_clicks / total_impressions) * 100, 2) if total_impressions else 0
        zones.append({
            "zone_id": zone_id,
            "zone_name": zone_name,
            "total_banners": total_banners,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": ctr
        })
        chart_labels.append(zone_name)
        chart_impressions.append(total_impressions)
        chart_clicks.append(total_clicks)

    return render(request, "advertiser_dashboard.html", {
        "advertiser": advertiser,
        "zones": zones,
        "chart_labels": chart_labels,
        "chart_impressions": chart_impressions,
        "chart_clicks": chart_clicks
    })





def banner_stats(request, banner_id):
    client = get_client()
    today = datetime.today().strftime("%Y-%m-%d")

    query = f"""
        SELECT 
        b.bannerid,
        b.description,
        SUM(m.latest_impressions) AS impressions,
        SUM(c.latest_clicks) AS clicks
    FROM rv_banners AS b
    LEFT JOIN (
        SELECT creative_id, MAX(count) AS latest_impressions
        FROM rv_data_bkt_m
        GROUP BY creative_id, interval_start
    ) m ON b.bannerid = m.creative_id
    LEFT JOIN (
        SELECT creative_id, MAX(count) AS latest_clicks
        FROM rv_data_bkt_c
        GROUP BY creative_id, interval_start
    ) c ON b.bannerid = c.creative_id
    WHERE b.bannerid = {banner_id}
    GROUP BY b.bannerid, b.description

    """
    result = client.query(query).result_rows

    banner = None
    if result:
        row = result[0]
        impressions = int(row[2] or 0)
        clicks = int(row[3] or 0)
        ctr = round((clicks / impressions) * 100, 2) if impressions > 0 else 0
        banner = {
            "id": row[0],
            "description": row[1],
            "impressions": impressions,
            "clicks": clicks,
            "ctr": ctr,
        }

    return render(request, "banner_stats.html", {"banner": banner})