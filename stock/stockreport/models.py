from django.db import models
from django.db.models import Sum
from django.contrib.auth.models import User
from django.utils import timezone
import uuid


class UserAPIConfig(models.Model):
    """User-specific API configuration for SQL Server connections"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='api_config')
    url = models.CharField(max_length=200, help_text="SQL Server host or address")
    database = models.CharField(max_length=100, help_text="SQL Server database name", default="your_db")
    username = models.CharField(max_length=100, help_text="SQL Server username")
    password = models.CharField(max_length=100, help_text="SQL Server password")
    port = models.CharField(max_length=10, null=True, blank=True)
    is_active = models.BooleanField(default=True, help_text="Whether this configuration is active for the user")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "User API Configuration"
        verbose_name_plural = "User API Configurations"

    def __str__(self):
        return f"API Config for {self.user.username} ({self.url}/{self.database})"


class UserSession(models.Model):
    """Track user sessions for single device login enforcement"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='sessions')
    session_key = models.CharField(max_length=40, unique=True)
    device_id = models.CharField(max_length=100, help_text="Unique device identifier")
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_activity = models.DateTimeField(auto_now=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "User Session"
        verbose_name_plural = "User Sessions"
        indexes = [
            models.Index(fields=['user', 'is_active']),
            models.Index(fields=['session_key']),
            models.Index(fields=['device_id']),
        ]

    def __str__(self):
        return f"Session for {self.user.username} on {self.device_id}"

    @classmethod
    def create_session(cls, user, session_key, device_id, ip_address=None, user_agent=""):
        """Create a new session and deactivate all other sessions for the user"""
        # Deactivate all existing sessions for this user
        cls.objects.filter(user=user, is_active=True).update(is_active=False)
        # Delete any session with the same session_key (to avoid unique constraint error)
        cls.objects.filter(session_key=session_key).delete()
        # Create new session
        return cls.objects.create(
            user=user,
            session_key=session_key,
            device_id=device_id,
            ip_address=ip_address,
            user_agent=user_agent,
            is_active=True
        )

    @classmethod
    def is_valid_session(cls, user, session_key, device_id):
        """Check if the session is valid for the user and device"""
        return cls.objects.filter(
            user=user,
            session_key=session_key,
            device_id=device_id,
            is_active=True
        ).exists()

    @classmethod
    def deactivate_session(cls, user, session_key):
        """Deactivate a specific session"""
        cls.objects.filter(
            user=user,
            session_key=session_key
        ).update(is_active=False)

    @classmethod
    def cleanup_expired_sessions(cls):
        """Remove expired sessions"""
        now = timezone.now()
        cls.objects.filter(
            expires_at__lt=now,
            is_active=True
        ).update(is_active=False)


# Legacy APIConfig model (kept for backward compatibility)
class APIConfig(models.Model):
    url = models.CharField(max_length=200, help_text="SQL Server host or address")
    database = models.CharField(max_length=100, help_text="SQL Server database name", default="your_db")
    username = models.CharField(max_length=100, help_text="SQL Server username")
    password = models.CharField(max_length=100, help_text="SQL Server password")
    port = models.CharField(max_length=10, null=True, blank=True)
    is_active = models.BooleanField(default=True, help_text="Only one configuration can be active at a time")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "API Configuration"
        verbose_name_plural = "API Configurations"

    def save(self, *args, **kwargs):
        if self.is_active:
            # Set all other configurations to inactive
            APIConfig.objects.exclude(pk=self.pk).update(is_active=False)
        super().save(*args, **kwargs)

    @classmethod
    def get_active_config(cls):
        return cls.objects.filter(is_active=True).first()

    def __str__(self):
        return f"SQL Config ({self.url}/{self.database})"

# Create your models here.
class master1(models.Model):
    name = models.CharField(max_length=100, verbose_name="Name")
    code = models.IntegerField(verbose_name="Code")
    mastertype = models.IntegerField(verbose_name="MasterType")

    class Meta:
        verbose_name = "Master"
        verbose_name_plural = "Masters"

    def __str__(self):
        return self.name
    
    def closing_stock(self):
        """Calculate closing stock for this master item"""
        total_quantity = self.tran2_set.aggregate(Sum('quantity'))['quantity__sum']
        return total_quantity if total_quantity is not None else 0
    
    @classmethod
    def get_masters_with_closing_stock(cls):
        """Get all masters with their closing stock calculated"""
        masters = cls.objects.all()
        for master in masters:
            master.closing_stock_value = master.closing_stock()
        return masters


class tran2(models.Model):
    mastercode1 = models.ForeignKey(master1, on_delete=models.CASCADE, verbose_name="Master Code")
    Vch_Type=models.IntegerField(verbose_name="Voucher Type")
    RecType = models.IntegerField(verbose_name="Record Type", default=0)
    date= models.DateField(verbose_name="Date")
    vch_no = models.CharField(max_length=50, verbose_name="Voucher No")
    quantity = models.FloatField(verbose_name="Quantity")
    amount = models.FloatField(verbose_name="Amount")
    
    class Meta:
        indexes = [
            models.Index(fields=['mastercode1', 'vch_no', 'date']),
            models.Index(fields=['RecType']),
        ]
    
    def __str__(self):
        return f'{str(self.vch_no)}- {str(self.mastercode1.name)}'
    
class folio1(models.Model):
    MasterCode = models.ForeignKey(master1, on_delete=models.CASCADE, verbose_name="Master Code")
    D1 = models.FloatField(verbose_name="Opening")
    D3 = models.FloatField(verbose_name="Opening Stock Value")

class tran1(models.Model):
    Vch_Type=models.IntegerField(verbose_name="Voucher Type")
    Date=models.DateField(verbose_name="Date")
    Vch_No=models.CharField(max_length=50, verbose_name="Voucher No")
    MasterCode1=models.ForeignKey(master1, on_delete=models.CASCADE, verbose_name="Master Code")
    Vch_amount=models.FloatField(verbose_name="Voucher Amount")

class ClosingStock(models.Model):
    master_item = models.ForeignKey(master1, on_delete=models.CASCADE, verbose_name="Master Item")
    opening_stock = models.FloatField(verbose_name="Opening Stock", default=0)
    opening_value = models.FloatField(verbose_name="Opening Value", default=0)
    closing_quantity = models.FloatField(verbose_name="Closing Quantity", default=0)
    closing_value = models.FloatField(verbose_name="Closing Value", default=0)
    calculated_date = models.DateTimeField(auto_now_add=True, verbose_name="Calculated Date")
    
    class Meta:
        verbose_name = "Closing Stock"
        verbose_name_plural = "Closing Stocks"
        unique_together = ['master_item']
    
    def __str__(self):
        return f"{self.master_item.name} - Qty: {self.closing_quantity}, Value: {self.closing_value}"

class ClosingBalance(models.Model):
    master_item = models.ForeignKey(master1, on_delete=models.CASCADE, verbose_name="Master Item")
    opening_balance = models.FloatField(verbose_name="Opening Balance", default=0)
    closing_balance = models.FloatField(verbose_name="Closing Balance", default=0)
    calculated_date = models.DateTimeField(auto_now_add=True, verbose_name="Calculated Date")
    
    class Meta:
        verbose_name = "Closing Balance"
        verbose_name_plural = "Closing Balances"
        unique_together = ['master_item']
    
    def __str__(self):
        return f"{self.master_item.name} - Opening: {self.opening_balance}, Closing: {self.closing_balance}"


class ItemParamDet(models.Model):
    Date = models.CharField(max_length=20)
    VchType = models.CharField(max_length=10)  # 2 = Purchase, 9 = Sale
    VchNo = models.CharField(max_length=10)
    Item = models.ForeignKey(master1, on_delete=models.CASCADE)
    C1 = models.CharField(max_length=50, blank=True)
    C2 = models.CharField(max_length=50, blank=True)
    C3 = models.CharField(max_length=50, blank=True)
    C4 = models.CharField(max_length=50, blank=True)
    C5 = models.CharField(max_length=50, blank=True)
    D3 = models.FloatField(blank=True, verbose_name="Mrp")
    D4 = models.FloatField(blank=True, verbose_name="sale price")
    BCN = models.CharField(max_length=50, blank=True)
    Value1 = models.FloatField(default=0, verbose_name="quantity")

    def __str__(self):
        return f"{self.Item.name} - {self.BCN} ({self.Date})"


def import_tran2_data(csv_file):
    """
    Import tran2 data from CSV file including RecType field
    
    Args:
        csv_file: File object containing CSV data with columns: 
                 mastercode1, Vch_Type, RecType, date, vch_no, quantity, amount
    
    Returns:
        dict: Summary of import results
    """
    import csv
    import io
    from datetime import datetime
    
    results = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'errors': [],
        'debug_info': []
    }
    
    try:
        csv_data = csv_file.read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                # Get master record
                try:
                    master = master1.objects.get(code=int(row['mastercode1']))
                except master1.DoesNotExist:
                    results['errors'].append(f"Row {row_num}: Master with code {row['mastercode1']} not found")
                    continue
                
                # Parse date
                try:
                    if '/' in row['date']:
                        date_obj = datetime.strptime(row['date'], '%d/%m/%Y').date()
                    else:
                        date_obj = datetime.strptime(row['date'], '%Y-%m-%d').date()
                except ValueError:
                    results['errors'].append(f"Row {row_num}: Invalid date format for {row['date']}")
                    continue
                
                # Get RecType value - check specifically for 'RecType' column
                rec_type_value = 0
                if 'RecType' in row:
                    try:
                        rec_type_value = int(row['RecType'])
                    except (ValueError, TypeError):
                        results['errors'].append(f"Row {row_num}: Invalid RecType value '{row['RecType']}' - must be a number")
                        continue
                else:
                    results['errors'].append(f"Row {row_num}: RecType column not found. Available columns: {list(row.keys())}")
                    continue
                
                # Add debug info for first few rows
                if row_num <= 5:
                    results['debug_info'].append(f"Row {row_num}: RecType value={rec_type_value}, raw value='{row['RecType']}', available columns={list(row.keys())}")
                
                # Prepare tran2 data
                tran_data = {
                    'mastercode1': master,
                    'Vch_Type': int(row.get('Vch_Type', 0)),
                    'RecType': rec_type_value,
                    'date': date_obj,
                    'vch_no': row['vch_no'].strip(),
                    'quantity': float(row.get('quantity', 0)),
                    'amount': float(row.get('amount', 0))
                }
                
                # Check if transaction already exists
                existing_tran = tran2.objects.filter(
                    mastercode1=master,
                    vch_no=tran_data['vch_no'],
                    date=tran_data['date']
                ).first()
                
                if existing_tran:
                    # Only update if RecType has changed
                    if existing_tran.RecType != tran_data['RecType']:
                        # Add debug info for RecType changes
                        if row_num <= 5:
                            results['debug_info'].append(f"Row {row_num}: Updating RecType from {existing_tran.RecType} to {tran_data['RecType']}")
                        
                        # Update existing transaction
                        for key, value in tran_data.items():
                            setattr(existing_tran, key, value)
                        existing_tran.save()
                        results['updated'] += 1
                    else:
                        # RecType hasn't changed, skip update
                        if row_num <= 5:
                            results['debug_info'].append(f"Row {row_num}: Skipping - RecType unchanged ({existing_tran.RecType})")
                        results['skipped'] = results.get('skipped', 0) + 1
                else:
                    # Create new transaction
                    tran2.objects.create(**tran_data)
                    results['created'] += 1
                    
            except (ValueError, KeyError) as e:
                results['errors'].append(f"Row {row_num}: {str(e)}")
            except Exception as e:
                results['errors'].append(f"Row {row_num}: Unexpected error - {str(e)}")
    
    except Exception as e:
        results['errors'].append(f"File processing error: {str(e)}")
    
    return results