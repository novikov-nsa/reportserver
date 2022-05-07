from django.contrib import admin
from .models import States, Tasks, Report

class TasksAdmin(admin.ModelAdmin):
    list_display = ('userLogin', 'report', 'reportParameters', 'reportParameters', 'startReportDateTime',
                    'endReportDateTime', 'state', 'fileName')
    list_display_links = ('report',)
    search_fields = ('userLogin', 'reportCode', 'reportName')


admin.site.register(Tasks, TasksAdmin)
admin.site.register(States)
admin.site.register(Report)

# Register your models here.
