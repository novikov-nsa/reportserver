# Generated by Django 4.0.4 on 2022-04-30 08:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('webapp', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='tasks',
            name='endReportDateTime',
            field=models.DateTimeField(null=True, verbose_name='Дата и время окончания'),
        ),
    ]
