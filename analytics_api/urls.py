# urls.py
from django.urls import path
from .views import advertiser_dashboard

urlpatterns = [
    path('advertiser_dashboard/', advertiser_dashboard, name='analytics'),
]
