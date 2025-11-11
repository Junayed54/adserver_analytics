from django.urls import path
from .views import *
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path("api/users/", UserListCreateView.as_view(), name="user-list-create"),
    path("api/users/<int:pk>/", UserDetailView.as_view(), name="user-detail"),
    
    path('api/token/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),  # login
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
]
