from django import forms
from .models import Tasks, States
from django.contrib.auth.models import User

class RunReport(forms.ModelForm):

    class Meta:
        model = Tasks
        fields = ('userLogin', 'report')
