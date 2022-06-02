from django.contrib import admin
from .models import States, Tasks, Report, ReportParameters

class TasksAdmin(admin.ModelAdmin):
    list_display = ('userLogin', 'report', 'reportParameters', 'reportParameters', 'startReportDateTime',
                    'endReportDateTime', 'state', 'fileName')
    list_display_links = ('report',)
    search_fields = ('userLogin', 'reportCode', 'reportName')


class ReportParametersAdmin(admin.ModelAdmin):
    list_display = ('report', 'parameter_systemname', 'parameter_name', 'parameter_type', 'parameter_required')
    list_display_links = ('report',)
    search_fields = ('parameter_systemname', 'parameter_name', 'parameter_type', 'parameter_required')

admin.site.register(Tasks, TasksAdmin)
admin.site.register(ReportParameters, ReportParametersAdmin)
admin.site.register(States)
admin.site.register(Report)


# Register your models here.
