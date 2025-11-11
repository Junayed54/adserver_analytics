# urls.py
from django.urls import path
from .templates_views import *

urlpatterns = [
    # New URL for all zones
    path('zones/', all_zones, name='all_zones'),
    path('admin_dashboard/', admin_dashboard, name='analytics'),
    path('advertisers/', advertiser_campaigns_view, name='all_advertisers'),
    path('advertiser/<int:advertiser_id>/', advertiser_dashboard, name='advertiser_dashboard'),
    path('publishers/', publisher_list, name="publisher_list"),
    path('publisher/<int:publisher_id>/dashboard/', publisher_dashboard, name='publisher_dashboard'),
    path("zone/<int:zone_id>/stats/", zone_stats, name="zone_stats"),
    path("banner/<int:banner_id>/stats/", banner_stats, name="banner_stats"),

]

from .views import *

urlpatterns += [
    path('api/admin-dashboard/', AdminDashboardAPIView.as_view(), name='admin-dashboard-api'),
    path('api/publisher-dashboard/<int:publisher_id>/', PublisherDashboardAPIView.as_view(), name='publisher-dashboard-api'),
    path('api/advertiser-dashboard/<int:advertiser_id>/', AdvertiserDashboardAPIView.as_view(), name='advertiser-dashboard-api'),
    path('api/accounts/', AccountListAPIView.as_view(), name='account-list-api'),

]
