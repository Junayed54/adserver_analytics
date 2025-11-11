from rest_framework import generics, permissions
from .models import CustomUser
from .serializers import UserSerializer, UserCreateSerializer


# List all users or create new ones
class UserListCreateView(generics.ListCreateAPIView):
    queryset = CustomUser.objects.all()

    def get_permissions(self):
        if self.request.method == "POST":  # Allow signup without auth
            return [permissions.AllowAny()]
        return [permissions.IsAuthenticated()]

    def get_serializer_class(self):
        if self.request.method == "POST":
            return UserCreateSerializer
        return UserSerializer

# Retrieve, update, delete a single user
class UserDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = CustomUser.objects.all()
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]


from rest_framework_simplejwt.views import TokenObtainPairView
from .serializers import CustomTokenObtainPairSerializer

class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer