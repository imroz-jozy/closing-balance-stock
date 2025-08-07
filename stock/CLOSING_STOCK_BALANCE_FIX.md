# Closing Stock and Closing Balance Date Filtering Fix

## Problem Description

The user reported that when entering a start date in the closing stock and closing balance reports, the system was not providing proper data. The issue was:

- **Without start date**: Only end date provided - worked correctly
- **With start date**: System did not properly calculate opening balance as of the start date

## Root Cause

The original logic in `views.py` was flawed:

1. It always used the `folio1` opening balance as the base
2. When start_date was provided, it only included transactions from start_date onwards
3. This meant it was not properly calculating the opening balance as of the start_date

## Solution

### Fixed Logic

The corrected logic now properly handles date filtering:

#### For Closing Stock Report (`closing_stock_report`)

1. **Opening Balance Calculation**:
   - If `start_date` is provided: Calculate balance up to (but not including) start_date
   - If no `start_date`: Use `folio1` opening balance

2. **Closing Balance Calculation**:
   - If `end_date` is provided: Calculate balance up to end_date
   - If no `end_date`: Calculate balance for all transactions

3. **Transaction Display**:
   - Show only transactions within the specified date range (if both dates provided)

#### For Closing Balance Report (`closing_balance_report`)

Same logic as closing stock report, but for account balances instead of inventory quantities.

### Code Changes

#### Before (Incorrect Logic)
```python
# Get opening balance from folio1
opening_quantity = folio_record.D1 or 0

# Filter transactions by date if filters are provided
transactions = tran2.objects.filter(mastercode1=item)
if start_date_obj:
    transactions = transactions.filter(date__gte=start_date_obj)
if end_date_obj:
    transactions = transactions.filter(date__lte=end_date_obj)

# Calculate final closing stock
final_quantity = round(opening_quantity + total_quantity, 2)
```

#### After (Correct Logic)
```python
# Get base opening balance from folio1
base_opening_quantity = folio_record.D1 or 0

# Calculate opening balance as of start_date (if provided)
if start_date_obj:
    # Get all transactions before start_date to calculate opening balance
    opening_transactions = tran2.objects.filter(
        mastercode1=item,
        date__lt=start_date_obj
    )
    opening_quantity = opening_transactions.aggregate(Sum('quantity'))['quantity__sum'] or 0
    opening_quantity = round(base_opening_quantity + opening_quantity, 2)
else:
    # Use folio1 opening balance if no start_date
    opening_quantity = base_opening_quantity

# Calculate closing balance up to end_date (if provided)
if end_date_obj:
    # Get all transactions up to end_date
    closing_transactions = tran2.objects.filter(
        mastercode1=item,
        date__lte=end_date_obj
    )
    closing_quantity = closing_transactions.aggregate(Sum('quantity'))['quantity__sum'] or 0
    closing_quantity = round(base_opening_quantity + closing_quantity, 2)
else:
    # Get all transactions if no end_date
    closing_transactions = tran2.objects.filter(mastercode1=item)
    closing_quantity = closing_transactions.aggregate(Sum('quantity'))['quantity__sum'] or 0
    closing_quantity = round(base_opening_quantity + closing_quantity, 2)
```

## Date Filtering Scenarios

### 1. No Dates Provided
- **Opening**: Uses `folio1` opening balance
- **Transactions**: Shows all transactions
- **Closing**: Opening + all transactions

### 2. Only End Date Provided
- **Opening**: Uses `folio1` opening balance
- **Transactions**: Shows transactions up to end date
- **Closing**: Opening + transactions up to end date

### 3. Only Start Date Provided
- **Opening**: Calculates balance as of start date (folio1 + transactions before start date)
- **Transactions**: Shows transactions from start date onwards
- **Closing**: Opening + all transactions

### 4. Both Start and End Dates Provided
- **Opening**: Calculates balance as of start date
- **Transactions**: Shows transactions within date range
- **Closing**: Calculates balance as of end date

## Testing

Comprehensive tests have been added in `tests.py` to verify all scenarios:

- `test_closing_stock_no_dates`
- `test_closing_stock_only_end_date`
- `test_closing_stock_only_start_date`
- `test_closing_stock_both_dates`
- `test_closing_balance_no_dates`
- `test_closing_balance_only_end_date`
- `test_closing_balance_only_start_date`
- `test_closing_balance_both_dates`

All tests pass and verify the correct calculation logic.

## Files Modified

1. **`stock/stockreport/views.py`**:
   - Fixed `closing_stock_report()` function
   - Fixed `closing_balance_report()` function
   - Added comprehensive documentation

2. **`stock/stockreport/tests.py`**:
   - Added comprehensive test suite
   - Tests all date filtering scenarios

## Usage

The fix is now active and users can:

1. Enter only an end date to see closing balance/stock as of that date
2. Enter only a start date to see opening balance/stock as of that date plus subsequent transactions
3. Enter both dates to see opening balance as of start date, transactions within range, and closing balance as of end date
4. Enter no dates to see all data

The system now correctly handles the opening balance calculation when a start date is provided, ensuring that all balances before the start date are properly included in the opening balance. 