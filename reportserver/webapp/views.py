from django.shortcuts import render
from django.http import HttpResponse
from django.urls import reverse_lazy

from .models import Tasks, States


def index(request):
    tasks = Tasks.objects.all()
    states = States.objects.all()
    content = {'tasks': tasks, 'states': states}
    return render(request, 'webapp/index.html', content)
