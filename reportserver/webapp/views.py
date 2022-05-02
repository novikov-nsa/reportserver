from django.shortcuts import render
from django.views.generic.edit import CreateView
from django.http import HttpResponse
from django.urls import reverse_lazy
from .forms import RunReport
from .models import Tasks, States

from .models import Tasks, States

class SetParamView(CreateView):
    template_name = 'webapp/setparamreport.html'
    form_class = RunReport
    success_url = reverse_lazy('index')


def index(request):
    tasks = Tasks.objects.all()
    states = States.objects.all()
    content = {'tasks': tasks, 'states': states}
    return render(request, 'webapp/index.html', content)
