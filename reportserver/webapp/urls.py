from django.urls import path
from django.contrib.auth.views import LoginView
from .views import index, run_report, download_file, logout_view, sel_report

urlpatterns = [
    path('accounts/login/', LoginView.as_view(), name='login'),
    path('runreport/', sel_report, name='selreport'),
    path('runreport/<int:pk>', run_report, name='runreport'),
    path('getreport/<int:pk>', download_file, name='getreport'),
    path('accounts/logout', logout_view, name='logoutview'),
    path('', index, name='index')
]


