import os
import datetime
import logging

from django.contrib.auth.models import User
from django.db import models
from django.conf import settings

from django.contrib.auth import authenticate
from django.db.models.signals import post_save
from django.dispatch import receiver

from treport.report import Report as XSLXReport
from treport.report import get_config

# Create your models here.

logger = logging.getLogger(__name__)

def get_new_state():
    item = States.objects.get(systemName='NEW')
    return item.pk

class Report(models.Model):
    def __str__(self):
        return self.name

    systemName = models.CharField(max_length=50, verbose_name='Системное имя отчетной формы', unique=True)
    name = models.CharField(max_length=250, verbose_name='Имя отчета на русском языке')

    class Meta:
        verbose_name_plural = 'Отчетные формы'
        verbose_name = 'Отчетная форма'
        ordering = ['systemName']

class ReportParameters(models.Model):
    def __str__(self):
        return self.parameter_name

    report = models.ForeignKey('Report', on_delete=models.PROTECT, verbose_name='Отчет')
    parameter_systemname = models.TextField(verbose_name="Системное имя параметра")
    parameter_name = models.TextField(verbose_name="Имя параметра на русском языке")
    parameter_type = models.TextField(verbose_name="Тип параметра")
    parameter_required = models.BooleanField(verbose_name='Обязательность при использовании')
    number_in_order = models.IntegerField(verbose_name="Номер параметра по порядку")

    class Meta:
        verbose_name_plural = 'Параметры отчетов'
        verbose_name = 'Параметр отчета'
        ordering = ['report', 'parameter_systemname']

class States(models.Model):
    def __str__(self):
        return self.name

    systemName = models.CharField(max_length=50, verbose_name='Системное имя статуса', unique=True)
    name = models.CharField(max_length=250, verbose_name='Имя статуса на русском языке')

    class Meta:
        verbose_name_plural = 'Статусы'
        verbose_name = 'Статус'
        ordering = ['systemName']

class Tasks(models.Model):
    userLogin = models.ForeignKey(User, editable=True, null=True, blank=True, on_delete=models.PROTECT,
                                  verbose_name="Пользователь, сформировавший запрос")
    report = models.ForeignKey('Report', on_delete=models.PROTECT, verbose_name='Отчет')
    reportParameters = models.TextField(verbose_name='Параметры формирования')
    startReportDateTime = models.DateTimeField(verbose_name='Дата и время запуска', auto_now_add=True, null=True, blank=True)
    endReportDateTime = models.DateTimeField(verbose_name='Дата и время окончания', null=True, blank=True)
    state = models.ForeignKey('States', on_delete=models.PROTECT, verbose_name='Статус', default=get_new_state())
    fileName = models.TextField(verbose_name='Имя файла', null=True, blank=True)
    reportContent = models.BinaryField(verbose_name='Отчет', null=True, blank=True)
    errorMsg = models.TextField(verbose_name='Сообщение об ошибке', null=True, blank=True)

    class Meta:
        verbose_name_plural = 'Задачи'
        verbose_name = 'Задача'
        ordering = ['-startReportDateTime']

@receiver(post_save, sender=Tasks)
def handler_report_run(sender, instance, **kwargs):
    if kwargs['created']:
        task = instance
        task.state = States.objects.get(systemName='FORMING')
        task.save()

        ini_file = os.path.join(settings.TREPORT_FILES, 'treport.ini')
        db_url, params_report_file = get_config(ini_file)
        path_to_params_report_file = os.path.join(settings.TREPORT_FILES, params_report_file)

        report_code = task.report.systemName
        param_values_list = task.reportParameters.split(';')

        params_values = {}
        for parameter in param_values_list:
            params_values[parameter.split('=')[0]] = parameter.split('=')[1].replace('\r', '')

        report = XSLXReport(report_code, path_to_params_report_file, params_values, db_url)

        if report.isCorrect:
            report.contentReport.save(report.outDir + report.report_file_name)
            with open(report.outDir + report.report_file_name, 'rb') as f:
                content = f.read()
            f.close()
            os.remove(report.outDir + report.report_file_name)

            task.reportContent = content
            task.fileName = report.report_file_name
            now = datetime.datetime.now()
            task.endReportDateTime = now
            task.state = States.objects.get(systemName='FORMED')
            task.save()
            logger.info(f'Сформированный отчет {report.report_file_name} сохранен а базе данных')
        else:
            now = datetime.datetime.now()
            task.endReportDateTime = now
            task.state = States.objects.get(systemName='ERROR')
            task.save()




