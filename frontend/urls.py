from django.urls import path
from django.views.generic import TemplateView

from .views import *
urlpatterns = [
    # path('', home, name="home"),
    path('', TemplateView.as_view(template_name="Html/src/html/index.html")),
    path('sign-in/', TemplateView.as_view(template_name="Html/src/html/sign-in-cover.html")),
    path('sign-up/', TemplateView.as_view(template_name="Html/src/html/sign-up-cover.html")),
    path('admin-dashboard/', TemplateView.as_view(template_name="analytics_api/admin.html")),
    path('publisher-dashboard/<int:publisher_id>/', TemplateView.as_view(template_name='analytics_api/publisher.html')),
    path('advertiser-dashboard/<int:advertiser_id>/', TemplateView.as_view(template_name='analytics_api/advertiser.html')),
]
