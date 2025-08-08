import pyodbc
from datetime import datetime
from .models import APIConfig, UserAPIConfig


class LiveSQLService:
    """Service class for querying SQL Server directly without importing data"""

    def __init__(self, user=None):
        self.user = user
        if user:
            # Use user-specific configuration
            self.api_config = UserAPIConfig.objects.filter(user=user, is_active=True).first()
        else:
            # Fallback to global configuration
            self.api_config = APIConfig.get_active_config()

        if not self.api_config:
            raise ValueError("No active SQL Server configuration found")

        # Construct connection string once
        if self.api_config.port and self.api_config.port.strip():
            server = f"{self.api_config.url},{self.api_config.port}"
        else:
            server = self.api_config.url

        self.conn_str = (
            f"DRIVER={{ODBC Driver 11 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={self.api_config.database};"
            f"UID={self.api_config.username};"
            f"PWD={self.api_config.password};"
            "TrustServerCertificate=yes;"
        )

    def get_connection(self):
        """Get SQL Server connection"""
        return pyodbc.connect(self.conn_str)

    def get_closing_stock_live(self, start_date=None, end_date=None, hide_zero_balance=False):
        """
        Get closing stock report directly from SQL Server

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            hide_zero_balance: If True, filter out items with zero total value

        Returns:
            list: List of dictionaries with stock data
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Base query for master items (type 6 = stock items)
            master_query = """
                SELECT Code, Name, MasterType 
                FROM Master1 
                WHERE MasterType = 6 
                ORDER BY Name
            """

            cursor.execute(master_query)
            master_items = [{'code': row[0], 'name': row[1], 'mastertype': row[2]} for row in cursor.fetchall()]

            results = []

            for item in master_items:
                # Get opening balance from folio1
                folio_query = """
                    SELECT D1, D3 
                    FROM folio1 
                    WHERE MasterCode = ? AND MasterType = 6
                """
                cursor.execute(folio_query, (item['code'],))
                folio_row = cursor.fetchone()

                opening_qty = folio_row[0] if folio_row and folio_row[0] else 0
                opening_val = folio_row[1] if folio_row and folio_row[1] else 0

                # Get transactions for this item
                tran_query = """
                    SELECT Date, Value1, Value3, VchType, RecType
                    FROM Tran2 
                    WHERE MasterCode1 = ?
                """
                params = [item['code']]

                if end_date:
                    tran_query += " AND Date <= ?"
                    params.append(end_date)

                tran_query += " ORDER BY Date, RecType"

                cursor.execute(tran_query, params)
                transactions = cursor.fetchall()

                # Calculate opening balance as of start_date if provided
                if start_date:
                    qty = opening_qty
                    val = opening_val
                    avg_rate = (val / qty) if qty else 0

                    for txn in transactions:
                        # Convert datetime to date for comparison
                        txn_date = txn[0].date() if hasattr(txn[0], 'date') else txn[0]
                        if txn_date >= start_date:
                            break
                        if txn[1] > 0:  # Purchase
                            qty += txn[1]
                            val += txn[2]
                            avg_rate = (val / qty) if qty else 0
                        elif txn[1] < 0:  # Sale
                            sale_qty = abs(txn[1])
                            val -= sale_qty * avg_rate
                            qty -= sale_qty
                            avg_rate = (val / qty) if qty else 0
                    opening_qty = qty
                    opening_val = val

                # Process transactions within the period
                period_qty = 0
                period_val = 0
                qty = opening_qty  # This is now the correct opening balance for the period
                val = opening_val  # This is now the correct opening value for the period
                avg_rate = (val / qty) if qty else 0

                for txn in transactions:
                    # Convert datetime to date for comparison
                    txn_date = txn[0].date() if hasattr(txn[0], 'date') else txn[0]
                    if start_date and txn_date < start_date:
                        continue
                    if end_date and txn_date > end_date:
                        continue
                    if txn[1] > 0:  # Purchase
                        qty += txn[1]
                        val += txn[2]
                        avg_rate = (val / qty) if qty else 0
                        period_qty += txn[1]
                        period_val += txn[2]
                    elif txn[1] < 0:  # Sale
                        sale_qty = abs(txn[1])
                        val -= sale_qty * avg_rate
                        qty -= sale_qty
                        avg_rate = (val / qty) if qty else 0
                        period_qty += txn[1]
                        period_val -= sale_qty * avg_rate

                closing_qty = qty
                closing_val = val

                # Include all items, with option to filter zero quantity items
                if not hide_zero_balance or closing_qty != 0:
                    results.append({
                        'item': {'name': item['name'], 'code': item['code']},
                        'opening_quantity': round(opening_qty, 2),
                        'opening_value': round(opening_val, 2),
                        'transaction_quantity': round(period_qty, 2),
                        'transaction_value': round(period_val, 2),
                        'closing_quantity': round(closing_qty, 2),
                        'closing_value': round(closing_val, 2),
                    })

            return results

    def get_closing_balance_live(self, start_date=None, end_date=None):
        """
        Get closing balance report directly from SQL Server

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            list: List of dictionaries with balance data
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Base query for master items (type 2 = account items)
            master_query = """
                SELECT Code, Name, MasterType 
                FROM Master1 
                WHERE MasterType = 2 
                ORDER BY Name
            """

            cursor.execute(master_query)
            master_items = [{'code': row[0], 'name': row[1], 'mastertype': row[2]} for row in cursor.fetchall()]

            results = []

            for item in master_items:
                # Get base opening balance from folio1
                folio_query = """
                    SELECT D1 
                    FROM folio1 
                    WHERE MasterCode = ? AND MasterType = 2
                """
                cursor.execute(folio_query, (item['code'],))
                folio_row = cursor.fetchone()

                base_opening_balance = folio_row[0] if folio_row and folio_row[0] else 0

                # Calculate opening balance as of start_date if provided
                if start_date:
                    opening_query = """
                        SELECT SUM(Value1) 
                        FROM Tran2 
                        WHERE MasterCode1 = ? AND Date < ?
                    """
                    cursor.execute(opening_query, (item['code'], start_date))
                    opening_amount = cursor.fetchone()[0] or 0
                    opening_balance = round(base_opening_balance + opening_amount, 2)
                else:
                    opening_balance = base_opening_balance

                # Calculate closing balance up to end_date if provided
                if end_date:
                    closing_query = """
                        SELECT SUM(Value1) 
                        FROM Tran2 
                        WHERE MasterCode1 = ? AND Date <= ?
                    """
                    cursor.execute(closing_query, (item['code'], end_date))
                    closing_amount = cursor.fetchone()[0] or 0
                    closing_balance = round(base_opening_balance + closing_amount, 2)
                else:
                    closing_query = """
                        SELECT SUM(Value1) 
                        FROM Tran2 
                        WHERE MasterCode1 = ?
                    """
                    cursor.execute(closing_query, (item['code'],))
                    closing_amount = cursor.fetchone()[0] or 0
                    closing_balance = round(base_opening_balance + closing_amount, 2)

                # Calculate transactions within the date range
                period_query = """
                    SELECT SUM(Value1) 
                    FROM Tran2 
                    WHERE MasterCode1 = ?
                """
                params = [item['code']]

                if start_date:
                    period_query += " AND Date >= ?"
                    params.append(start_date)

                if end_date:
                    period_query += " AND Date <= ?"
                    params.append(end_date)

                cursor.execute(period_query, params)
                period_amount = cursor.fetchone()[0] or 0

                # Only include accounts with non-zero total value
                total_value = abs(opening_balance) + abs(period_amount) + abs(closing_balance)
                if total_value > 0:
                    results.append({
                        'item': {'name': item['name'], 'code': item['code']},
                        'opening_balance': opening_balance,
                        'transaction_amount': period_amount,
                        'closing_balance': closing_balance
                    })

            return results 

    def get_stock_ledger_live(self, master_code, start_date=None, end_date=None):
        """
        Get detailed stock ledger for a specific item directly from SQL Server

        Args:
            master_code: The master code of the item
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            list: List of dictionaries with ledger data
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get item details
            master_query = """
                SELECT Code, Name, MasterType 
                FROM Master1 
                WHERE Code = ? AND MasterType = 6
            """
            cursor.execute(master_query, (master_code,))
            master_row = cursor.fetchone()

            if not master_row:
                return []

            # Get opening balance from folio1
            folio_query = """
                SELECT D1, D3 
                FROM folio1 
                WHERE MasterCode = ? AND MasterType = 6
            """
            cursor.execute(folio_query, (master_code,))
            folio_row = cursor.fetchone()

            opening_qty = folio_row[0] if folio_row and folio_row[0] else 0
            opening_val = folio_row[1] if folio_row and folio_row[1] else 0

            # Get all transactions for this item
            all_tran_query = """
                SELECT Date, Value1, Value3, VchType, RecType, VchNo
                FROM Tran2 
                WHERE MasterCode1 = ?
                ORDER BY Date, RecType
            """
            cursor.execute(all_tran_query, (master_code,))
            all_transactions = cursor.fetchall()

            # Calculate opening balance as of start_date if provided
            if start_date:
                qty = opening_qty
                val = opening_val
                avg_rate = (val / qty) if qty else 0

                for txn in all_transactions:
                    # Convert datetime to date for comparison
                    txn_date = txn[0].date() if hasattr(txn[0], 'date') else txn[0]
                    if txn_date >= start_date:
                        break
                    if txn[1] > 0:  # Purchase
                        qty += txn[1]
                        val += txn[2]
                        avg_rate = (val / qty) if qty else 0
                    elif txn[1] < 0:  # Sale
                        sale_qty = abs(txn[1])
                        val -= sale_qty * avg_rate
                        qty -= sale_qty
                        avg_rate = (val / qty) if qty else 0

                opening_qty = qty
                opening_val = val

            # Filter transactions for the period
            transactions = []
            for txn in all_transactions:
                txn_date = txn[0].date() if hasattr(txn[0], 'date') else txn[0]
                if start_date and txn_date < start_date:
                    continue
                if end_date and txn_date > end_date:
                    continue
                transactions.append(txn)

            ledger_data = []

            # Calculate running balances
            running_qty = opening_qty
            running_val = opening_val
            avg_rate = (running_val / running_qty) if running_qty else 0

            # Add opening balance as first row if we have opening data
            if opening_qty > 0 or opening_val > 0:
                ledger_data.append({
                    'sno': 0,
                    'date': start_date if start_date else (transactions[0][0] if transactions else datetime.now().date()),
                    'vchno': 'Opening Balance',
                    'opamount': 0,
                    'opqty': 0,
                    'qtyin': opening_qty,
                    'qtyout': 0,
                    'closingqty': opening_qty,
                    'closingamt': opening_val,
                    'description': f'Opening Balance as of {start_date}' if start_date else 'Opening Balance'
                })

            # Process each transaction
            for idx, txn in enumerate(transactions, 1):
                # Calculate opening values for this transaction
                op_qty = running_qty
                op_val = running_val

                # Determine qty in/out
                if txn[1] > 0:  # Value1 is quantity
                    qty_in = txn[1]
                    qty_out = 0
                    # Purchase - update running balances
                    running_qty += txn[1]
                    running_val += txn[2]  # Value3 is amount
                    avg_rate = (running_val / running_qty) if running_qty else 0
                else:
                    qty_in = 0
                    qty_out = abs(txn[1])
                    # Sale - update running balances
                    sale_qty = abs(txn[1])
                    running_val -= sale_qty * avg_rate
                    running_qty -= sale_qty

                ledger_data.append({
                    'sno': idx,
                    'date': txn[0].date() if hasattr(txn[0], 'date') else txn[0],
                    'vchno': str(txn[5]) if txn[5] else f"TXN-{idx}",
                    'opamount': round(op_val, 2),
                    'opqty': round(op_qty, 2),
                    'qtyin': round(qty_in, 2),
                    'qtyout': round(qty_out, 2),
                    'closingqty': round(running_qty, 2),
                    'closingamt': round(running_val, 2),
                    'description': f"Voucher Type: {txn[3]}, RecType: {txn[4]}"
                })

            return ledger_data 

    def get_item_details(self, master_code):
        """
        Fetch item details from SQL Server by code.
        Returns a dict with keys: code, name, mastertype, id (id=code for compatibility)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT Code, Name, MasterType FROM Master1 WHERE Code = ? AND MasterType = 6""", (master_code,)
            )
            row = cursor.fetchone()
            if row:
                return {'code': row[0], 'name': row[1], 'mastertype': row[2], 'id': row[0]}
            return None 

    def get_item_param_stock_live(self, item_id=None, bcn=None, search=None):
        """
        Get item parameter stock data from live SQL Server with proper item dropdown support
        """
        try:
            # Build the query
            query = """
            SELECT 
                p.ItemCode as item_id,
                i.Name as item_name,
                p.VchType as vch_type,
                p.C1, p.C2, p.C3, p.C4, p.C5,
                p.D3 as mrp,
                p.D4 as sale_price,
                p.BCN as bcn,
                p.Value1 as quantity
            FROM itemParamDet p
            INNER JOIN Master1 i ON i.Code = p.ItemCode
            WHERE i.MasterType = 6
            """

            params = []

            # Apply filters
            if item_id:
                query += " AND p.ItemCode = ?"
                params.append(item_id)

            if bcn:
                query += " AND p.BCN LIKE ?"
                params.append(f"%{bcn}%")

            if search:
                query += " AND (i.Name LIKE ? OR p.BCN LIKE ? OR p.C1 LIKE ? OR p.C2 LIKE ? OR p.C3 LIKE ? OR p.C4 LIKE ? OR p.C5 LIKE ?)"
                search_param = f"%{search}%"
                params.extend([search_param] * 7)

            query += " ORDER BY i.Name, p.BCN"

            # Execute query
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                columns = [column[0] for column in cursor.description]

                results = []
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))
                    # Ensure all values are properly converted
                    row_dict['quantity'] = float(row_dict.get('quantity', 0) or 0)
                    row_dict['mrp'] = float(row_dict.get('mrp', 0) or 0)
                    row_dict['sale_price'] = float(row_dict.get('sale_price', 0) or 0)
                    row_dict['vch_type'] = str(row_dict.get('vch_type', '') or '')
                    # Handle None values for other fields
                    for key in ['C1', 'C2', 'C3', 'C4', 'C5', 'bcn']:
                        row_dict[key] = row_dict.get(key) or ''
                    results.append(row_dict)

                print(f"Found {len(results)} records for item_id={item_id}, bcn={bcn}, search={search}")
                return results

        except Exception as e:
            print(f"Error in get_item_param_stock_live: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def get_item_param_det_live(self):
        """
        Fetch all itemParamDet data from SQL Server for import
        Returns:
            List of dicts with itemParamDet data
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT Date, VchType, VchNo, ItemCode, C1, C2, C3, C4, C5, D3, D4, BCN, Value1
                FROM itemParamDet
                ORDER BY Date, VchNo
            '''
            cursor.execute(query)
            rows = cursor.fetchall()

            # Prepare results
            results = []
            for row in rows:
                results.append({
                    'Date': row[0],
                    'VchType': row[1],
                    'VchNo': row[2],
                    'ItemCode': row[3],
                    'C1': row[4],
                    'C2': row[5],
                    'C3': row[6],
                    'C4': row[7],
                    'C5': row[8],
                    'D3': row[9],
                    'D4': row[10],
                    'BCN': row[11],
                    'Value1': row[12],
                })
            return results