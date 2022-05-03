from django.contrib import admin
from .models import States, Tasks

class TasksAdmin(admin.ModelAdmin):
    list_display = ('userLogin', 'reportCode', 'reportName', 'reportParameters', 'reportParameters', 'startReportDateTime',
                    'endReportDateTime', 'state', 'fileName')
    list_display_links = ('reportCode', 'reportName')
    search_fields = ('userLogin', 'reportCode', 'reportName')

    def save_model(self, request, obj, form, change):
        if not obj.created_by:
            obj.created_by = request.user
        obj.save()

admin.site.register(Tasks, TasksAdmin)
admin.site.register(States)

# Register your models here.
