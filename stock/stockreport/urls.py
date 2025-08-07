from django.urls import path
from . import views

urlpatterns = [
    # Authentication URLs
    path('login/', views.user_login, name='login'),
    path('signup/', views.user_signup, name='signup'),
    path('logout/', views.user_logout, name='logout'),
    path('profile/', views.user_profile, name='user_profile'),
    path('check-session/', views.check_session_validity, name='check_session_validity'),
    
    # Main application URLs
    path('', views.home, name='home'),
    path('closing-stock/', views.closing_stock_report, name='closing_stock_report'),
    path('closing-balance/', views.closing_balance_report, name='closing_balance_report'),
    path('stock-ledger/<int:master_id>/', views.stock_ledger_report, name='stock_ledger_report'),
    path('import-from-desktop/', views.import_from_desktop, name='import_from_desktop'),
    path('item-param-stock/', views.item_param_stock_report, name='item_param_stock_report'),
    path('item-param-summary/', views.item_param_summary_report, name='item_param_summary_report'),
    
    # Export endpoints
    path('export-closing-stock-excel/', views.export_closing_stock_excel, name='export_closing_stock_excel'),
    path('export-closing-stock-pdf/', views.export_closing_stock_pdf, name='export_closing_stock_pdf'),
    path('export-closing-balance-excel/', views.export_closing_balance_excel, name='export_closing_balance_excel'),
    path('export-closing-balance-pdf/', views.export_closing_balance_pdf, name='export_closing_balance_pdf'),
    path('export-stock-ledger-excel/<int:master_id>/', views.export_stock_ledger_excel, name='export_stock_ledger_excel'),
    path('export-stock-ledger-pdf/<int:master_id>/', views.export_stock_ledger_pdf, name='export_stock_ledger_pdf'),
]