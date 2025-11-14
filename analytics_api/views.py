from django.shortcuts import render
import clickhouse_connect
from datetime import datetime

from rest_framework.views import APIView
from rest_framework.response import Response



def get_client():
    # return clickhouse_connect.get_client(
    #     host="localhost",
    #     port=8123,
    #     username="revive_user",
    #     password="revive_pass",
    #     database="revive_db"
    # )
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="re_click_server"
    )
    
    
    
class AdminDashboardAPIView(APIView):
    def get(self, request, format=None):
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

        data = {
            "total_advertisers": result[0],
            "total_campaigns": result[1],
            "total_banners": result[2],
            "total_zones": result[3],
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": ctr
        }

        return Response(data)



class PublisherDashboardAPIView(APIView):
    def get(self, request, publisher_id):
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
            LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.zoneid = z.zoneid
            LEFT JOIN revive_db.rv_banners AS b ON b.bannerid = s.ad_id
            LEFT JOIN revive_db.rv_campaigns AS c ON c.campaignid = b.campaignid
            LEFT JOIN revive_db.rv_clients AS cl ON cl.clientid = c.clientid
            WHERE p.account_id = {publisher_id}
            GROUP BY p.account_id, p.account_name
        """

        result_info = client.query(query_info).result_rows

        publisher = {}
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
            LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.zoneid = z.zoneid
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

        return Response({
            "publisher": publisher,
            "zones": zones,
            "chart_labels": chart_labels,
            "chart_impressions": chart_impressions,
            "chart_clicks": chart_clicks
        })

class AdvertiserDashboardAPIView(APIView):
    def get(self, request, advertiser_id):
        client = get_client()

        # --- 1. Advertiser Summary ---
        query_info = f"""
            SELECT 
                cl.clientid AS advertiser_id,
                cl.clientname AS name,
                COUNT(DISTINCT c.campaignid) AS total_campaigns,
                COUNT(DISTINCT b.bannerid) AS total_banners,
                COUNT(DISTINCT z.zoneid) AS total_zones,
                COALESCE(SUM(s.impressions), 0) AS total_impressions,
                COALESCE(SUM(s.clicks), 0) AS total_clicks
            FROM revive_db.rv_clients AS cl
            LEFT JOIN revive_db.rv_campaigns AS c ON cl.clientid = c.clientid
            LEFT JOIN revive_db.rv_banners AS b ON b.campaignid = c.campaignid
            LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.ad_id = b.bannerid
            LEFT JOIN revive_db.rv_zones AS z ON z.zoneid = s.zone_id
            WHERE cl.clientid = {advertiser_id}
            GROUP BY cl.clientid, cl.clientname
        """

        result_info = client.query(query_info).result_rows

        advertiser = {}
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

        # --- 2. Zone-wise Breakdown ---
        query_zones = f"""
            SELECT 
                z.zoneid AS zone_id,
                z.zonename AS zone_name,
                COUNT(DISTINCT b.bannerid) AS total_banners,
                COALESCE(SUM(s.impressions), 0) AS total_impressions,
                COALESCE(SUM(s.clicks), 0) AS total_clicks
            FROM revive_db.rv_zones AS z
            LEFT JOIN revive_db.rv_data_summary_ad_hourly AS s ON s.zoneid = z.zoneid
            LEFT JOIN revive_db.rv_banners AS b ON b.bannerid = s.ad_id
            LEFT JOIN revive_db.rv_campaigns AS c ON c.campaignid = b.campaignid
            WHERE c.clientid = {advertiser_id}
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

        return Response({
            "advertiser": advertiser,
            "zones": zones,
            "chart_labels": chart_labels,
            "chart_impressions": chart_impressions,
            "chart_clicks": chart_clicks
        })
        
        
        
class AccountListAPIView(APIView):
    def get(self, request):
        client = get_client()

        # --- Publishers ---
        query_publishers = """
            SELECT 
                account_id AS id,
                account_name AS name,
                account_type,
                'publisher' AS type
            FROM revive_db.rv_accounts
            ORDER BY account_name ASC
        """
        publisher_rows = client.query(query_publishers).result_rows
        publishers = [
            {
                "id": row[0],
                "name": row[1],
                "account_type": row[2],
                "type": row[3]
            }
            for row in publisher_rows
        ]

        # --- Advertisers ---
        query_advertisers = """
            SELECT 
                clientid AS id,
                clientname AS name,
                contact,
                email,
                updated,
                'advertiser' AS type
            FROM revive_db.rv_clients
            ORDER BY clientname ASC
        """
        advertiser_rows = client.query(query_advertisers).result_rows
        advertisers = [
            {
                "id": row[0],
                "name": row[1],
                "contact": row[2],
                "email": row[3],
                "updated": row[4].isoformat() if row[4] else None,
                "type": row[5]
            }
            for row in advertiser_rows
        ]

        return Response({
            "publishers": publishers,
            "advertisers": advertisers
        })
