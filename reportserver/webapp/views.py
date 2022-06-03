import os.path
import logging

from django.shortcuts import render
from django.http import HttpResponseRedirect, FileResponse
from django.views.generic.edit import CreateView
from django.contrib.auth.decorators import login_required
from django.urls import reverse_lazy
from .forms import RunReport, RunReportParameters

from django.contrib.auth.models import User
from django.contrib.auth import logout
from django import forms
from django.conf import settings

from .models import Tasks, States

logger = logging.getLogger(__name__)


@login_required(login_url='accounts/login/')
def run_report(request):
    if request.method == 'POST':
        form = RunReport(request.POST or None)
        if form.is_valid():
            task = form.save(commit=False)
            user = User.objects.get(username=request.user.username)
            task.userLogin = user
            task.report = form.cleaned_data['report']
            task.reportParameters = form.cleaned_data['reportParameters']
            task.save()

            return HttpResponseRedirect('/')
    else:
        form = RunReport()
        form.fields['userLogin'].widget = forms.HiddenInput()
        form_parameters = RunReportParameters()

    return render(request, 'webapp/setparamreport.html', {'form': form})


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