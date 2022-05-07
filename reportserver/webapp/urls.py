from django.urls import path
from django.contrib.auth.views import LoginView
from .views import index, run_report, download_file

urlpatterns = [
    path('accounts/login/', LoginView.as_view(), name='login'),
    path('runreport/', run_report, name='runreport'),
    path('getreport/<int:pk>', download_file, name='getreport'),
    path('', index, name='index')
]


