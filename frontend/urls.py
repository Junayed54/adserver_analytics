from django.urls import path
from django.views.generic import TemplateView

from .views import *
urlpatterns = [
    # path('', home, name="home"),
    path('', TemplateView.as_view(template_name="Html/src/html/index.html")),
    path('sign-in/', TemplateView.as_view(template_name="Html/src/html/sign-in-cover.html")),
    path('sign-up/', TemplateView.as_view(template_name="Html/src/html/sign-up-cover.html")),
    path('admin_dash/', TemplateView.as_view(template_name="admin.html")),
    path('publisher_dash/<int:publisher_id>/', TemplateView.as_view(template_name='publisher.html')),
    path('advertiser_dash/<int:advertiser_id>/', TemplateView.as_view(template_name='advertiser.html')),
]
