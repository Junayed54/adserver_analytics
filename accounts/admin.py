from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.translation import gettext_lazy as _
from .models import CustomUser


@admin.register(CustomUser)
class CustomUserAdmin(BaseUserAdmin):
    # Fields to display in list view
    list_display = ("email", "first_name", "last_name", "role", "revive_id", "is_staff", "is_active")
    list_filter = ("role", "is_staff", "is_active")
    search_fields = ("email", "first_name", "last_name", "advertiser_id", "publisher_id")
    ordering = ("email",)

    # Fields to show in detail view
    fieldsets = (
        (None, {"fields": ("email", "password")}),
        (_("Personal info"), {"fields": ("first_name", "last_name")}),
        (_("Role & IDs"), {"fields": ("role", "advertiser_id", "publisher_id")}),
        (_("Permissions"), {"fields": ("is_active", "is_staff", "is_superuser", "groups", "user_permissions")}),
        (_("Important dates"), {"fields": ("last_login", "date_joined")}),
    )

    # Fields to show when adding a new user
    add_fieldsets = (
        (None, {
            "classes": ("wide",),
            "fields": ("email", "first_name", "last_name", "role", "advertiser_id", "publisher_id", "password1", "password2", "is_staff", "is_active"),
        }),
    )

    # Custom method to show correct revive_id
    def revive_id(self, obj):
        return obj.get_revive_id()
    revive_id.short_description = "Revive ID"
