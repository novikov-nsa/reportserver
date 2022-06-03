import os.path
import logging
import datetime

from django.shortcuts import render
from django.http import HttpResponseRedirect, FileResponse
from django.views.generic.edit import CreateView
from django.contrib.auth.decorators import login_required
from django.urls import reverse_lazy
from .forms import RunReport

from django.contrib.auth.models import User
from django.contrib.auth import logout
from django import forms
from django.conf import settings

from .models import Tasks, States, Report, ReportParameters

logger = logging.getLogger(__name__)

@login_required(login_url='accounts/login/')
def sel_report(request):
    if request.method == 'POST':
        form = RunReport(request.POST or None)
        if form.is_valid():
            task = form.save(commit=False)
            report_pk = task.report.pk
            return HttpResponseRedirect(str(report_pk))
    else:
        form = RunReport()
        form.fields['userLogin'].widget = forms.HiddenInput()
    return render(request, 'webapp/sel_report.html', {'form': form})

@login_required(login_url='accounts/login/')
def run_report(request, pk):
    if request.method == 'POST':
        task = Tasks()
        task.report = Report.objects.get(pk=pk)
        task.state = States.objects.get(systemName='NEW')
        task.startReportDateTime = datetime.datetime.now()

        list_parameters = []
        for parametr in request.POST:
            if parametr != 'csrfmiddlewaretoken':
                list_parameters.append(parametr+"="+request.POST[parametr])
        task.reportParameters = ";".join(list_parameters)
        task.userLogin = User.objects.get_by_natural_key(request.user.username)
        task.save()
        return HttpResponseRedirect('/')
    else:
        report = Report.objects.get(pk=pk)
        report_name = report.name
        report_parameters = ReportParameters.objects.filter(report=report).order_by('number_in_order')
        parameters = {}
        for parameter in report_parameters:
            parameters[parameter.parameter_systemname] = parameter.parameter_name


        content = {'parameters_report': parameters, 'report_name': report_name}


    return render(request, 'webapp/setparamreport.html', content)






@login_required(login_url='accounts/login/')
def index(request):
    user = User.objects.get(username=request.user.username)
    tasks = Tasks.objects.filter(userLogin=user)
    states = States.objects.all()
    content = {'tasks': tasks, 'states': states}
    return render(request, 'webapp/index.html', content)

@login_required(login_url='accounts/login/')
def download_file(request, pk):
    task = Tasks.objects.get(pk=pk)
    filename = str(pk)+'_'+task.fileName
    tmp_file = os.path.join(settings.TMP_FILES, filename)
    w_file = open(tmp_file, 'wb+')
    w_file.write(task.reportContent)
    w_file.close()
    return FileResponse(open(tmp_file, 'rb'), content_type="application/octet-stream")

def logout_view(request):
    logout(request)
    return HttpResponseRedirect('/')