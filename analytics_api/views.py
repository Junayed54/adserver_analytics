# # views.py
# from django.shortcuts import render
# import clickhouse_connect

# def analytics_view(request):
#     client = clickhouse_connect.get_client(host='localhost')

#     # The query should use the correct table name: revive_impressions
#     query = '''
#         SELECT toDate(timestamp) AS day, sum(impressions) AS impressions
#         FROM revive_impressions
#         GROUP BY day
#         ORDER BY day ASC
#     '''
#     result = client.query(query)
    
#     dates = [row[0].strftime('%Y-%m-%d') for row in result.result_rows]
#     impressions = [row[1] for row in result.result_rows]

#     return render(request, 'analytics.html', {
#         'dates': dates,
#         'impressions': impressions
#     })



from django.shortcuts import render
from clickhouse_connect import get_client

def analytics_view(request):
    # Connect to ClickHouse with credentials and database
    client = get_client(
        host='localhost',
        port=8123,
        username='revive_user',
        password='revive_pass',
        database='revive_db'
    )

    # Query to aggregate impressions by day
    query = '''
        SELECT toDate(timestamp) AS day, sum(impressions) AS impressions
        FROM revive_impressions
        GROUP BY day
        ORDER BY day ASC
    '''

    result = client.query(query)

    # Extract and format data for the template
    dates = [row[0].strftime('%Y-%m-%d') for row in result.result_rows]
    impressions = [row[1] for row in result.result_rows]

    return render(request, 'analytics.html', {
        'dates': dates,
        'impressions': impressions
    })
