from django.db import models

# Create your models here.
def get_new_state():
    item = States.objects.get(systemName='NEW')
    return item.pk

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
    userLogin = models.CharField(max_length=200, verbose_name='Логин пользователя')
    reportCode = models.CharField(max_length=50, verbose_name='Код отчета')
    reportName = models.TextField(verbose_name='Наименование отчета')
    reportParameters = models.TextField(verbose_name='Параметры формирования')
    startReportDateTime = models.DateTimeField(verbose_name='Дата и время запуска', auto_now_add=True, null=True, blank=True)
    endReportDateTime = models.DateTimeField(verbose_name='Дата и время окончания', null=True, blank=True)
    state = models.ForeignKey('States', on_delete=models.PROTECT, verbose_name='Статус', default=get_new_state())
    fileName = models.TextField(verbose_name='Имя файла', null=True, blank=True)
    reportContetn = models.BinaryField(verbose_name='Отчет', null=True, blank=True)

    class Meta:
        verbose_name_plural = 'Задачи'
        verbose_name = 'Задача'
        ordering = ['-startReportDateTime']



