# Generated by Django 4.0.4 on 2022-05-03 07:08

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('webapp', '0008_alter_tasks_userlogin'),
    ]

    operations = [
        migrations.AlterField(
            model_name='tasks',
            name='userLogin',
            field=models.ForeignKey(blank=True, default=None, editable=False, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL, verbose_name='Пользователь, сформировавший запрос'),
        ),
    ]