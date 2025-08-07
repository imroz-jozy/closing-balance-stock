from django.contrib import admin, messages
from django.db.models import Sum
from django.urls import path, reverse
from django.shortcuts import redirect, render
from django.http import HttpResponseRedirect
from .models import master1, tran2, APIConfig, UserAPIConfig, UserSession, folio1, tran1, ClosingStock, ClosingBalance
import requests
import xml.etree.ElementTree as ET
from django.utils.html import format_html
import datetime
import time
import pyodbc


@admin.register(UserAPIConfig)
class UserAPIConfigAdmin(admin.ModelAdmin):
    list_display = ('user', 'url', 'database', 'username', 'is_active', 'created_at', 'updated_at')
    list_filter = ('is_active', 'created_at', 'updated_at')
    search_fields = ('user__username', 'url', 'database', 'username')
    readonly_fields = ('created_at', 'updated_at')


@admin.register(UserSession)
class UserSessionAdmin(admin.ModelAdmin):
    list_display = ('user', 'device_id', 'ip_address', 'is_active', 'created_at', 'last_activity')
    list_filter = ('is_active', 'created_at', 'last_activity')
    search_fields = ('user__username', 'device_id', 'ip_address')
    readonly_fields = ('created_at', 'last_activity')
    
    def has_add_permission(self, request):
        return False  # Sessions are created automatically
    
    def has_change_permission(self, request, obj=None):
        return False  # Sessions should not be manually edited


@admin.register(APIConfig)
class APIConfigAdmin(admin.ModelAdmin):
    list_display = ('url', 'username', 'is_active', 'created_at', 'updated_at')
    list_filter = ('is_active', 'created_at')
    search_fields = ('url', 'username')


@admin.register(master1)
class Master1Admin(admin.ModelAdmin):
    list_display = ('name', 'get_closing_stock')

    def get_closing_stock(self, obj):
        return obj.closing_stock()

    get_closing_stock.short_description = 'Closing Stock'

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'import-from-desktop/',
                self.admin_site.admin_view(self.import_from_desktop),
                name='import-from-desktop',
            ),
        ]
        return custom_urls + urls

    def import_from_desktop(self, request):
        # Run import logic on both GET and POST requests
        try:
            # Step 1: Import Master1 data FIRST
            master_xml_response = execute_query(query_type='master1')
            if not master_xml_response:
                messages.error(request, "Failed to fetch Master1 data from external source.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            master_parsed_data = master_xml_response
            if not master_parsed_data:
                messages.error(request, "No Master1 data found or failed to parse XML response.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            old_master1_count = master1.objects.count()
            master1.objects.all().delete()
            messages.info(request, f"Cleared {old_master1_count} master1 records")

            master_imported_count = 0
            for entry in master_parsed_data:
                try:
                    master1.objects.create(
                        code=int(entry['Code']),
                        name=entry['Name'],
                        mastertype=int(entry['MasterType'])
                    )
                    master_imported_count += 1
                except Exception as e:
                    messages.warning(request, f"Error importing master1 record: {str(e)}")

            messages.success(request, f"Successfully imported {master_imported_count} master1 records")

            # Step 2: Import Tran2 data (AFTER master1)
            start_time = time.time()
            initial_tran2_count = tran2.objects.count()
            messages.info(request, f"Starting Tran2 import. Current database has {initial_tran2_count} records.")
            xml_response = execute_query(query_type='tran2')
            if not xml_response:
                messages.error(request, "Failed to fetch Tran2 data from external source.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            parsed_data = xml_response
            if not parsed_data:
                messages.error(request, "No Tran2 data found or failed to parse XML response.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            existing_records = {}
            for record in tran2.objects.select_related('mastercode1').all():
                key = (record.mastercode1.code, record.vch_no, record.date)
                existing_records[key] = record

            imported_count = 0
            updated_count = 0
            skipped_count = 0
            error_count = 0

            for entry in parsed_data:
                try:
                    master_code = int(entry['MasterCode1'])
                    if not master1.objects.filter(code=master_code).exists():
                        messages.warning(request, f"Skipped Tran2 record with non-existent master code {master_code} for vch_no {entry.get('VchNo', 'unknown')}")
                        error_count += 1
                        continue
                    item = master1.objects.get(code=master_code)
                    date_obj = entry['Date']
                    if not date_obj:
                        messages.warning(request, f"Skipped Tran2 record with missing date for vch_no {entry.get('VchNo', 'unknown')}")
                        error_count += 1
                        continue
                    if isinstance(date_obj, str):
                        try:
                            date_obj = datetime.datetime.strptime(date_obj, "%Y-%m-%d").date()
                        except ValueError:
                            try:
                                date_obj = datetime.datetime.strptime(date_obj, "%Y-%m-%dT%H:%M:%S").date()
                            except ValueError:
                                messages.warning(request, f"Skipped Tran2 record with invalid date '{date_obj}' for vch_no {entry.get('VchNo', 'unknown')}")
                                error_count += 1
                                continue
                    elif isinstance(date_obj, (datetime.datetime, datetime.date)):
                        date_obj = date_obj.date() if isinstance(date_obj, datetime.datetime) else date_obj
                    else:
                        messages.warning(request, f"Skipped Tran2 record with unsupported date type for vch_no {entry.get('VchNo', 'unknown')}")
                        error_count += 1
                        continue
                    new_rec_type = int(entry['RecType'])
                    lookup_key = (item.code, entry['VchNo'].strip(), date_obj)
                    existing_tran = existing_records.get(lookup_key)
                    if imported_count + updated_count + skipped_count < 5:
                        if existing_tran:
                            messages.info(request, f"Found existing record: Master={item.code}, VchNo={entry['VchNo']}, OldRecType={existing_tran.RecType}, NewRecType={new_rec_type}")
                        else:
                            messages.info(request, f"No existing record found: Master={item.code}, VchNo={entry['VchNo']}, NewRecType={new_rec_type}")
                    if existing_tran:
                        if new_rec_type > existing_tran.RecType:
                            existing_tran.Vch_Type = int(entry['VchType'])
                            existing_tran.RecType = new_rec_type
                            existing_tran.quantity = float(entry['Value1'])
                            existing_tran.amount = float(entry['Value3'])
                            existing_tran.save()
                            updated_count += 1
                        else:
                            skipped_count += 1
                    else:
                        tran2.objects.create(
                            mastercode1=item,
                            Vch_Type=int(entry['VchType']),
                            RecType=new_rec_type,
                            vch_no=entry['VchNo'].strip(),
                            date=date_obj,
                            quantity=float(entry['Value1']),
                            amount=float(entry['Value3']),
                        )
                        imported_count += 1
                except Exception as e:
                    error_count += 1
                    messages.warning(request, f"Error importing Tran2 record: {str(e)}")

            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            final_tran2_count = tran2.objects.count()
            total_change = final_tran2_count - initial_tran2_count
            messages.success(request, f"Tran2 import completed: {imported_count} created, {updated_count} updated, {skipped_count} skipped. Time taken: {minutes} min {seconds} sec. Database: {initial_tran2_count} → {final_tran2_count} records (+{total_change})")
            if error_count > 0:
                messages.warning(request, f"{error_count} Tran2 records had errors during import.")

            # Step 3: Import Folio1 data (AFTER master1)
            folio1_xml_response = execute_query(query_type='folio1')
            if not folio1_xml_response:
                messages.error(request, "Failed to fetch Folio1 data from external source.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            folio1_parsed_data = folio1_xml_response
            if not folio1_parsed_data:
                messages.error(request, "No Folio1 data found or failed to parse XML response.")
                return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))

            old_folio1_count = folio1.objects.count()
            folio1.objects.all().delete()
            messages.info(request, f"Cleared {old_folio1_count} folio1 records")

            folio1_imported_count = 0
            folio1_error_count = 0
            for entry in folio1_parsed_data:
                try:
                    master_instance = master1.objects.get(code=int(entry['MasterCode']))
                    folio1.objects.create(
                        MasterCode=master_instance,
                        D1=entry['D1'],
                        D3=entry['D3']
                    )
                    folio1_imported_count += 1
                except Exception as e:
                    folio1_error_count += 1
                    messages.warning(request, f"Error importing folio1 record: {str(e)}")

            messages.success(request, f"Successfully imported {folio1_imported_count} folio1 records")
            if folio1_error_count > 0:
                messages.warning(request, f"{folio1_error_count} folio1 records had errors during import.")

        except Exception as e:
            messages.error(request, f"Import failed: {str(e)}")
        return HttpResponseRedirect(reverse('admin:stockreport_master1_changelist'))


def execute_query(query_type='tran2'):
    api_config = APIConfig.get_active_config()
    if not api_config:
        raise ValueError("No active SQL Server configuration found")

    queries = {
        'tran2': "SELECT MasterCode1,VchType,RecType,VchNo,Date,Value1,Value3 FROM Tran2",
        'master1': "SELECT Code, MasterType, Name FROM Master1 WHERE MasterType IN (2,6,9)",
        'folio1': "SELECT MasterCode, MasterType, D1, D3 FROM folio1 WHERE MasterType in (2,6)"
    }
    query = queries[query_type]

    # Handle named instance (if port is blank) or host:port
    if api_config.port and api_config.port.strip():
        server = f"{api_config.url},{api_config.port}"
    else:
        server = api_config.url

    conn_str = (
        f"DRIVER={{ODBC Driver 11 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={api_config.database};"
        f"UID={api_config.username};"
        f"PWD={api_config.password};"
        "TrustServerCertificate=yes;"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@admin.register(tran2)
class Tran2Admin(admin.ModelAdmin):
    list_display = ('mastercode1', 'Vch_Type', 'RecType', 'date', 'vch_no', 'quantity', 'amount')
    list_filter = ('Vch_Type', 'RecType', 'date')
    search_fields = ('vch_no', 'mastercode1__name')


@admin.register(tran1)
class Tran1Admin(admin.ModelAdmin):
    list_display = ('Vch_No', 'Date')


@admin.register(ClosingStock)
class ClosingStockAdmin(admin.ModelAdmin):
    list_display = ('master_item', 'opening_stock', 'opening_value', 'closing_quantity', 'closing_value', 'calculated_date')
    list_filter = ('calculated_date',)
    search_fields = ('master_item__name', 'master_item__code')
    readonly_fields = ('calculated_date',)
    
    def has_add_permission(self, request):
        return False  # Disable manual addition, only allow through import process
    
    def has_delete_permission(self, request, obj=None):
        return False  # Disable manual deletion, only allow through import process


@admin.register(ClosingBalance)
class ClosingBalanceAdmin(admin.ModelAdmin):
    list_display = ('master_item', 'opening_balance', 'closing_balance', 'calculated_date')
    list_filter = ('calculated_date',)
    search_fields = ('master_item__name', 'master_item__code')
    readonly_fields = ('calculated_date',)
    
    def has_add_permission(self, request):
        return False  # Disable manual addition, only allow through import process
    
    def has_delete_permission(self, request, obj=None):
        return False  # Disable manual deletion, only allow through import process
