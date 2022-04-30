# Generated by Django 4.0.4 on 2022-04-30 08:16

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='States',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('systemName', models.CharField(max_length=50, unique=True, verbose_name='Системное имя статуса')),
                ('name', models.CharField(max_length=250, verbose_name='Имя статуса на русском языке')),
            ],
            options={
                'verbose_name': 'Статус',
                'verbose_name_plural': 'Статусы',
                'ordering': ['systemName'],
            },
        ),
        migrations.CreateModel(
            name='Tasks',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('userLogin', models.CharField(max_length=200, verbose_name='Логин пользователя')),
                ('reportCode', models.CharField(max_length=50, verbose_name='Код отчета')),
                ('reportName', models.TextField(verbose_name='Наименование отчета')),
                ('reportParameters', models.TextField(verbose_name='Параметры формирования')),
                ('startReportDateTime', models.DateTimeField(verbose_name='Дата и время запуска')),
                ('endReportDateTime', models.DateTimeField(verbose_name='Дата и время окончания')),
                ('fileName', models.TextField(verbose_name='Имя файла')),
                ('reportContetn', models.BinaryField(verbose_name='Отчет')),
                ('state', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='webapp.states', verbose_name='Статус')),
            ],
            options={
                'verbose_name': 'Задача',
                'verbose_name_plural': 'Задачи',
                'ordering': ['-startReportDateTime'],
            },
        ),
    ]
