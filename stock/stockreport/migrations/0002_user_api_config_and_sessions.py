# Generated manually for user-specific API configurations and session management

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('stockreport', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserAPIConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('url', models.CharField(help_text='SQL Server host or address', max_length=200)),
                ('database', models.CharField(default='your_db', help_text='SQL Server database name', max_length=100)),
                ('username', models.CharField(help_text='SQL Server username', max_length=100)),
                ('password', models.CharField(help_text='SQL Server password', max_length=100)),
                ('port', models.CharField(blank=True, max_length=10, null=True)),
                ('is_active', models.BooleanField(default=True, help_text='Whether this configuration is active for the user')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='api_config', to='auth.user')),
            ],
            options={
                'verbose_name': 'User API Configuration',
                'verbose_name_plural': 'User API Configurations',
            },
        ),
        migrations.CreateModel(
            name='UserSession',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('session_key', models.CharField(max_length=40, unique=True)),
                ('device_id', models.CharField(help_text='Unique device identifier', max_length=100)),
                ('ip_address', models.GenericIPAddressField(blank=True, null=True)),
                ('user_agent', models.TextField(blank=True)),
                ('is_active', models.BooleanField(default=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('last_activity', models.DateTimeField(auto_now=True)),
                ('expires_at', models.DateTimeField(blank=True, null=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='sessions', to='auth.user')),
            ],
            options={
                'verbose_name': 'User Session',
                'verbose_name_plural': 'User Sessions',
            },
        ),
        migrations.AddIndex(
            model_name='usersession',
            index=models.Index(fields=['user', 'is_active'], name='stockreport__user_id_8f9c8c_idx'),
        ),
        migrations.AddIndex(
            model_name='usersession',
            index=models.Index(fields=['session_key'], name='stockreport__session_9c8c8c_idx'),
        ),
        migrations.AddIndex(
            model_name='usersession',
            index=models.Index(fields=['device_id'], name='stockreport__device__8c8c8c_idx'),
        ),
    ] 