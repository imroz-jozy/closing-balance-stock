from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Run VACUUM on the SQLite database to reclaim unused space.'

    def handle(self, *args, **options):
        self.stdout.write('Running VACUUM on the SQLite database...')
        with connection.cursor() as cursor:
            cursor.execute('VACUUM;')
        self.stdout.write(self.style.SUCCESS('VACUUM completed.')) 