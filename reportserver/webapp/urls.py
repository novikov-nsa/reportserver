from django.urls import path
from .views import index, SetParamView

urlpatterns = [
    path('runreport/', SetParamView.as_view(), name='runreport'),
    path('', index, name='index')
]


