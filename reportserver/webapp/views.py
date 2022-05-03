from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.views.generic.edit import CreateView
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.urls import reverse_lazy
from .forms import RunReport
from .models import Tasks, States
from django.contrib.auth.models import User
from django import forms

from .models import Tasks, States


@login_required(login_url='accounts/login/')
def run_report(request):
    if request.method == 'POST':
        form = RunReport(request.POST or None)
        if form.is_valid():
            task = form.save(commit=False)
            user = User.objects.get(username=request.user.username)
            task.userLogin = user
            task.reportCode = form.cleaned_data['reportCode']
            task.reportName = form.cleaned_data['reportName']
            task.reportParameters = form.cleaned_data['reportParameters']
            task.save()
            return HttpResponseRedirect('/')
    else:
        form = RunReport()
        form.fields['userLogin'].widget = forms.HiddenInput()

    return render(request, 'webapp/setparamreport.html', {'form': form})


@login_required(login_url='accounts/login/')
def index(request):
    tasks = Tasks.objects.all()
    states = States.objects.all()
    content = {'tasks': tasks, 'states': states}
    return render(request, 'webapp/index.html', content)