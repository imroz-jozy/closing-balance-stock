"""URL configuration for stock project."""

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('stockreport.urls')),  # Add this line
]
