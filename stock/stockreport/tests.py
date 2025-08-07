from django.test import TestCase
from django.test import Client
from django.urls import reverse
from django.utils import timezone
from datetime import datetime, date
from .models import master1, tran2, folio1, ClosingStock, ClosingBalance


class ClosingStockBalanceTests(TestCase):
    """Test cases for closing stock and closing balance logic with date filtering"""
    
    def setUp(self):
        """Set up test data"""
        # Create test master items
        self.inventory_item = master1.objects.create(
            code=1001,
            name="Test Inventory Item",
            mastertype=6  # Inventory item
        )
        
        self.account_item = master1.objects.create(
            code=2001,
            name="Test Account",
            mastertype=2  # Account
        )
        
        # Create folio1 records for opening balances
        folio1.objects.create(
            MasterCode=self.inventory_item,
            D1=100.0,  # Opening quantity
            D3=1000.0  # Opening value
        )
        
        folio1.objects.create(
            MasterCode=self.account_item,
            D1=500.0,  # Opening balance
            D3=0.0     # Opening value (not used for accounts but required)
        )
        
        # Create test transactions
        # Transaction 1: Before start date
        tran2.objects.create(
            mastercode1=self.inventory_item,
            Vch_Type=1,
            RecType=1,
            vch_no="VCH001",
            date=date(2024, 1, 15),
            quantity=50.0,
            amount=500.0
        )
        
        tran2.objects.create(
            mastercode1=self.account_item,
            Vch_Type=1,
            RecType=1,
            vch_no="VCH001",
            date=date(2024, 1, 15),
            quantity=200.0,
            amount=0.0
        )
        
        # Transaction 2: Within date range
        tran2.objects.create(
            mastercode1=self.inventory_item,
            Vch_Type=2,
            RecType=1,
            vch_no="VCH002",
            date=date(2024, 2, 15),
            quantity=-20.0,
            amount=-200.0
        )
        
        tran2.objects.create(
            mastercode1=self.account_item,
            Vch_Type=2,
            RecType=1,
            vch_no="VCH002",
            date=date(2024, 2, 15),
            quantity=-50.0,
            amount=0.0
        )
        
        # Transaction 3: After end date
        tran2.objects.create(
            mastercode1=self.inventory_item,
            Vch_Type=1,
            RecType=1,
            vch_no="VCH003",
            date=date(2024, 3, 15),
            quantity=30.0,
            amount=300.0
        )
        
        tran2.objects.create(
            mastercode1=self.account_item,
            Vch_Type=1,
            RecType=1,
            vch_no="VCH003",
            date=date(2024, 3, 15),
            quantity=100.0,
            amount=0.0
        )
        
        self.client = Client()
    
    def test_closing_stock_no_dates(self):
        """Test closing stock report with no date filters"""
        response = self.client.get(reverse('closing_stock_report'))
        self.assertEqual(response.status_code, 200)
        
        # Should show all transactions
        # Opening: 100 + 50 = 150
        # Transactions: 50 + (-20) + 30 = 60
        # Closing: 100 + 50 + (-20) + 30 = 160
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_quantity'], 100.0)  # From folio1
        self.assertEqual(item_result['opening_value'], 1000.0)    # From folio1
        self.assertEqual(item_result['transaction_quantity'], 60.0)  # All transactions
        self.assertEqual(item_result['transaction_value'], 600.0)    # All transactions
        self.assertEqual(item_result['closing_quantity'], 160.0)     # Opening + all transactions
        self.assertEqual(item_result['closing_value'], 1600.0)       # Opening + all transactions
    
    def test_closing_stock_only_end_date(self):
        """Test closing stock report with only end date"""
        response = self.client.get(reverse('closing_stock_report'), {
            'end_date': '2024-02-28'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show transactions up to end date
        # Opening: 100 (from folio1)
        # Transactions: 50 + (-20) = 30
        # Closing: 100 + 50 + (-20) = 130
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_quantity'], 100.0)  # From folio1
        self.assertEqual(item_result['opening_value'], 1000.0)    # From folio1
        self.assertEqual(item_result['transaction_quantity'], 30.0)  # Transactions up to end date
        self.assertEqual(item_result['transaction_value'], 300.0)    # Transactions up to end date
        self.assertEqual(item_result['closing_quantity'], 130.0)     # Opening + transactions up to end date
        self.assertEqual(item_result['closing_value'], 1300.0)       # Opening + transactions up to end date
    
    def test_closing_stock_only_start_date(self):
        """Test closing stock report with only start date"""
        response = self.client.get(reverse('closing_stock_report'), {
            'start_date': '2024-02-01'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show opening balance as of start date + transactions from start date
        # Opening as of start date: 100 + 50 = 150
        # Transactions from start date: (-20) + 30 = 10
        # Closing: 100 + 50 + (-20) + 30 = 160
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_quantity'], 150.0)  # Opening + transactions before start date
        self.assertEqual(item_result['opening_value'], 1500.0)    # Opening + transactions before start date
        self.assertEqual(item_result['transaction_quantity'], 10.0)  # Transactions from start date
        self.assertEqual(item_result['transaction_value'], 100.0)    # Transactions from start date
        self.assertEqual(item_result['closing_quantity'], 160.0)     # Opening + all transactions
        self.assertEqual(item_result['closing_value'], 1600.0)       # Opening + all transactions
    
    def test_closing_stock_both_dates(self):
        """Test closing stock report with both start and end dates"""
        response = self.client.get(reverse('closing_stock_report'), {
            'start_date': '2024-02-01',
            'end_date': '2024-02-28'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show opening balance as of start date + transactions within range + closing as of end date
        # Opening as of start date: 100 + 50 = 150
        # Transactions within range: (-20) = -20
        # Closing as of end date: 100 + 50 + (-20) = 130
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_quantity'], 150.0)  # Opening + transactions before start date
        self.assertEqual(item_result['opening_value'], 1500.0)    # Opening + transactions before start date
        self.assertEqual(item_result['transaction_quantity'], -20.0)  # Transactions within range
        self.assertEqual(item_result['transaction_value'], -200.0)    # Transactions within range
        self.assertEqual(item_result['closing_quantity'], 130.0)     # Opening + transactions up to end date
        self.assertEqual(item_result['closing_value'], 1300.0)       # Opening + transactions up to end date
    
    def test_closing_balance_no_dates(self):
        """Test closing balance report with no date filters"""
        response = self.client.get(reverse('closing_balance_report'))
        self.assertEqual(response.status_code, 200)
        
        # Should show all transactions
        # Opening: 500
        # Transactions: 200 + (-50) + 100 = 250
        # Closing: 500 + 200 + (-50) + 100 = 750
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_balance'], 500.0)  # From folio1
        self.assertEqual(item_result['transaction_amount'], 250.0)  # All transactions
        self.assertEqual(item_result['closing_balance'], 750.0)     # Opening + all transactions
    
    def test_closing_balance_only_end_date(self):
        """Test closing balance report with only end date"""
        response = self.client.get(reverse('closing_balance_report'), {
            'end_date': '2024-02-28'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show transactions up to end date
        # Opening: 500 (from folio1)
        # Transactions: 200 + (-50) = 150
        # Closing: 500 + 200 + (-50) = 650
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_balance'], 500.0)  # From folio1
        self.assertEqual(item_result['transaction_amount'], 150.0)  # Transactions up to end date
        self.assertEqual(item_result['closing_balance'], 650.0)     # Opening + transactions up to end date
    
    def test_closing_balance_only_start_date(self):
        """Test closing balance report with only start date"""
        response = self.client.get(reverse('closing_balance_report'), {
            'start_date': '2024-02-01'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show opening balance as of start date + transactions from start date
        # Opening as of start date: 500 + 200 = 700
        # Transactions from start date: (-50) + 100 = 50
        # Closing: 500 + 200 + (-50) + 100 = 750
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_balance'], 700.0)  # Opening + transactions before start date
        self.assertEqual(item_result['transaction_amount'], 50.0)  # Transactions from start date
        self.assertEqual(item_result['closing_balance'], 750.0)     # Opening + all transactions
    
    def test_closing_balance_both_dates(self):
        """Test closing balance report with both start and end dates"""
        response = self.client.get(reverse('closing_balance_report'), {
            'start_date': '2024-02-01',
            'end_date': '2024-02-28'
        })
        self.assertEqual(response.status_code, 200)
        
        # Should show opening balance as of start date + transactions within range + closing as of end date
        # Opening as of start date: 500 + 200 = 700
        # Transactions within range: (-50) = -50
        # Closing as of end date: 500 + 200 + (-50) = 650
        results = response.context['results']
        self.assertEqual(len(results), 1)
        
        item_result = results[0]
        self.assertEqual(item_result['opening_balance'], 700.0)  # Opening + transactions before start date
        self.assertEqual(item_result['transaction_amount'], -50.0)  # Transactions within range
        self.assertEqual(item_result['closing_balance'], 650.0)     # Opening + transactions up to end date
