from rest_framework import serializers
from .models import CustomUser


class UserSerializer(serializers.ModelSerializer):
    revive_id = serializers.SerializerMethodField()

    class Meta:
        model = CustomUser
        fields = [
            "id",
            "email",
            "first_name",
            "last_name",
            "role",
            "advertiser_id",
            "publisher_id",
            "revive_id",
            "date_joined",
        ]
        read_only_fields = ["id", "date_joined", "revive_id"]

    def get_revive_id(self, obj):
        return obj.get_revive_id()


class UserCreateSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = CustomUser
        fields = [
            "email",
            "password",
            "first_name",
            "last_name",
            "role",
            "advertiser_id",
            "publisher_id",
        ]

    def validate(self, data):
        role = data.get("role")

        if role == CustomUser.Role.ADVERTISER and not data.get("advertiser_id"):
            raise serializers.ValidationError("Advertiser ID is required for advertisers.")
        if role == CustomUser.Role.PUBLISHER and not data.get("publisher_id"):
            raise serializers.ValidationError("Publisher ID is required for publishers.")

        return data

    def create(self, validated_data):
        password = validated_data.pop("password")
        user = CustomUser.objects.create_user(**validated_data)
        user.set_password(password)
        user.save()
        return user



from rest_framework_simplejwt.serializers import TokenObtainPairSerializer

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        # Add custom claims
        token['role'] = user.role
        # print(user.get_revive_id())
        token['revive_id'] = user.get_revive_id()
        
        return token

    def validate(self, attrs):
        data = super().validate(attrs)
        data['role'] = self.user.role
        data['revive_id'] = self.user.get_revive_id()
        return data
