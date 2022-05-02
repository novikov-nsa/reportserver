from django.forms import ModelForm
from .models import Tasks, States

class RunReport(ModelForm):
    class Meta:
        model = Tasks
        fields = ('reportCode', 'reportName', 'reportParameters')