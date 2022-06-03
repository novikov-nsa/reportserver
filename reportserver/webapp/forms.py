from django import forms
from .models import Tasks, States
from django.contrib.auth.models import User

class RunReport(forms.ModelForm):

    class Meta:
        model = Tasks
        fields = ('userLogin', 'report', 'reportParameters')

class RunReportParameters(forms.Form):
    fields_form = {'p_start_date': {
                    'field_type': 'text',
                    'label': 'Дата начала отчетного периода'},
                'p_end_date': {
                    'field_type': 'text',
                    'label': 'Дата окончания отчетного периода'},
            }
    for field_form in fields_form:
        field_form = forms.Field(label=fields_form[field_form]['label'])