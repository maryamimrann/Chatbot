import gradio as gr
import pandas as pd
from pymongo import MongoClient
import dateutil.parser
import re
import calendar
from datetime import datetime
from functools import lru_cache

# Debugging 
DEBUG = True
def debug_print(*args):
    if DEBUG:
        print("DEBUG:", *args)

# ---- Helper Functions ----
def format_currency(amount):
    """Format currency with proper symbols and commas"""
    try:
        if pd.isna(amount):
            return "$0.00"
        if isinstance(amount, str):
            amount = amount.replace('$', '').replace(',', '')
        return f"${float(amount):,.2f}"
    except (ValueError, TypeError):
        return "$0.00"

def format_order_details(order):
    """Format order details into a readable string"""
    details = [
        f"📄 **Order Details**",
        f"- Requisition #: {order.get('Requisition Number', 'N/A')}",
        f"- PO #: {order.get('Purchase Order Number', 'N/A')}",
        f"- Supplier: {str(order.get('Supplier Name', 'N/A')).title()}",
        f"- Item: {order.get('Item Name', 'N/A')}",
        f"- Description: {order.get('Item Description', 'N/A')}",
        f"- Quantity: {order.get('Quantity', 'N/A')}",
        f"- Unit Price: {format_currency(order.get('Unit Price', 0))}",
        f"- Total Price: {format_currency(order.get('Total Price', 0))}",
        f"- Date: {order.get('Purchase Date', 'N/A')}",
        f"- Department: {order.get('Department Name', 'N/A')}",
        f"- Location: {order.get('Location', 'N/A')}"
    ]
    return "\n".join(details)

# ---- MongoDB Connection ----
def get_db_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.server_info()
        return client['orderData']['sample']
    except Exception as e:
        debug_print(f"MongoDB connection failed: {e}")
        return None

# ---- Data Loading ----
global_df = None

@lru_cache(maxsize=1)
def load_procurement_data():
    global global_df
    try:
        collection = get_db_connection()
        if collection is None:
            return pd.DataFrame()
        data = list(collection.find({}, {'_id': 0}))
        df = pd.DataFrame(data)
        if df.empty:
            debug_print("Warning: Empty DataFrame")
            return df
        
        # Data cleaning
        df['Purchase Date'] = pd.to_datetime(df['Purchase Date'], format="%m/%d/%Y", errors='coerce')
        valid_years = list(range(2012, 2016))
        df = df[df['Purchase Date'].dt.year.isin(valid_years)]
        
        # Add derived columns
        df['Year'] = df['Purchase Date'].dt.year
        df['Month'] = df['Purchase Date'].dt.month
        df['Quarter'] = df['Purchase Date'].dt.quarter
        
        # Clean numeric columns
        df['Total Price'] = pd.to_numeric(df['Total Price'].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce')
        df['Unit Price'] = pd.to_numeric(df['Unit Price'].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce')
        
        # Clean text columns
        df['Supplier Name'] = df['Supplier Name'].astype(str).str.strip().str.lower()
        df['Supplier Zip Code'] = df['Supplier Zip Code'].astype(str).str.strip()
        df['Supplier Qualifications'] = df['Supplier Qualifications'].astype(str).str.strip().str.upper()
        
        df['CalCard'] = df['CalCard'].astype(str).str.upper().str.strip()
        
        return df
    except Exception as e:
        debug_print(f"Data loading error: {e}")
        return pd.DataFrame()

# ---- Question Logic ----
def month_str_to_number(month_name):
    try:
        return list(calendar.month_name).index(month_name.capitalize())
    except ValueError:
        try:
            return pd.to_datetime(month_name, format='%B').month
        except:
            return None
        

def handle_question(question):
    df = load_procurement_data()
    if df.empty:
        return "⚠️ Could not load data. Please check database connection."

    question = question.lower()

    # ===== NEW QUESTION HANDLERS =====
 
    if any(phrase in question for phrase in ["purchases delivered to zip", "purchases in zip", "orders delivered to"]):
        zip_match = re.search(r"(?:zip code|zip|location)\s*(\d{5})", question)
        if zip_match:
            zip_code = zip_match.group(1)
            # Search in both Location and Supplier Zip Code columns
            location_mask = df['Location'].astype(str).str.contains(zip_code)
            supplier_zip_mask = df['Supplier Zip Code'].astype(str).str.contains(zip_code)
            purchases = df[location_mask | supplier_zip_mask]

            if not purchases.empty:
                return "📍 Purchases for location {}:\n{}".format(
                    zip_code,
                    "\n".join([
                        f"- {row['Item Name']} (${row['Total Price']:,.2f}, {row['Purchase Date'].strftime('%Y-%m-%d')})"
                        for _, row in purchases.head(20).iterrows()
                    ])
                )  # Properly closed parentheses
            else:
                return f"⚠️ No purchases found for ZIP code {zip_code}"

    # ===== PURCHASES BY ACQUISITION METHOD =====
    elif "purchases used the" in question and "acquisition method" in question:
        method_match = re.search(r"the (.+?) acquisition method", question)
        if method_match:
            method = method_match.group(1).strip()
            year_match = re.search(r"in (\d{4})", question)

            if year_match:
                year = int(year_match.group(1))
                method_mask = df['Acquisition Method'].str.contains(method, case=False)
                year_mask = df['Year'] == year
                count = df[method_mask & year_mask].shape[0]
                return f"📦 Number of {method} purchases in {year}: {count}"
            else:
                count = df[df['Acquisition Method'].str.contains(method, case=False)].shape[0]
                return f"📦 Total {method} purchases: {count}"
    
    # ===== TOP N SUPPLIERS BY SPEND =====
    if "top" in question and "suppliers" in question and "total spend" in question:
        # Extract the number (default to 3 if not specified)
        num_match = re.search(r"top (\d+)", question)
        n = int(num_match.group(1)) if num_match else 3

        # Check if year is specified
        year_match = re.search(r"in (\d{4})", question)
        if year_match:
            year = int(year_match.group(1))
            suppliers = df[df['Year'] == year].groupby('Supplier Name')['Total Price']\
                        .sum().nlargest(n)
            time_period = f"in {year}"
        else:
            suppliers = df.groupby('Supplier Name')['Total Price']\
                        .sum().nlargest(n)
            time_period = "overall"

        if not suppliers.empty:
            return "🏆 Top {} suppliers {}:\n{}".format(
                n,
                time_period,
                "\n".join([f"{i+1}. {supplier}: ${amt:,.2f}" 
                        for i, (supplier, amt) in enumerate(suppliers.items())])
            )  # Properly closed parentheses
        else:
            return f"⚠️ No supplier data found {time_period}"

    # ===== SUPPLIER CODE SPENDING =====
    elif "supplier code" in question and ("total price" in question or "total spend" in question):
        code_match = re.search(r"supplier code (\d+)", question)
        if code_match:
            code = code_match.group(1).strip()
            # Convert both to strings for comparison
            supplier_data = df[df['Supplier Code'].astype(str).str.strip() == code]

            if not supplier_data.empty:
                total = supplier_data['Total Price'].sum()
                supplier_name = supplier_data.iloc[0]['Supplier Name']
                return (f"🏢 Total spend for supplier code {code} ({supplier_name}): "
                        f"${total:,.2f}\n"
                        f"- Number of purchases: {len(supplier_data)}")
            else:
                return f"⚠️ No purchases found for supplier code {code}"

    # ===== HIGHEST SPEND ACQUISITION TYPE =====
    elif "acquisition type had the highest spend" in question:
        year_match = re.search(r"in (\d{4})", question)
        if year_match:
            year = int(year_match.group(1))
            df_year = df[df['Year'] == year]
        else:
            df_year = df

        if not df_year.empty:
            top_type = df_year.groupby('Acquisition Type')['Total Price'].sum().nlargest(1)
            return (f"📊 Highest spending acquisition type "
                    f"{'in '+str(year) if year_match else ''}:\n"
                    f"- {top_type.index[0]}: ${top_type.iloc[0]:,.2f}")
        else:
            return f"⚠️ No data found for specified year"

    # ===== FREQUENTLY PURCHASED ITEMS =====
    elif "most frequently purchased items" in question and "fiscal year" in question:
        year_match = re.search(r"fiscal year (\d{4})", question)
        if year_match:
            year = year_match.group(1)
            fiscal_mask = df['Fiscal Year'].str.contains(year, na=False)
            top_items = df[fiscal_mask]['Item Name'].value_counts().head(10)

            if not top_items.empty:
                return "🛒 Most frequently purchased items in FY{}:\n{}".format(
                    year,
                    "\n".join([f"- {item} ({count})" 
                            for item, count in top_items.items()])
                )  # Properly closed parentheses
            else:
                return f"⚠️ No purchase data found for FY{year}"

    # ===== NORMALIZED UNSPSC CODE =====
    elif "normalized unspsc for" in question:
        item = re.search(r"for (.+?)\??$", question).group(1).strip().upper()
        items = df[df['Item Name'].str.upper().str.contains(item, na=False)]

        if not items.empty:
            unspsc = items.iloc[0]['Normalized UNSPSC']
            return f"🏷️ Normalized UNSPSC for {item}: {unspsc}"
        else:
            return f"⚠️ No UNSPSC code found for {item}"

    # ===== TRANSACTIONS BY SUB-ACQUISITION METHOD =====
    elif "transactions with sub-acquisition method" in question:
        method = re.search(r"method (.+?)\??$", question).group(1).strip()
        transactions = df[df['Sub-Acquisition Method'].str.contains(method, case=False, na=False)]

        if not transactions.empty:
            return "📝 Transactions with sub-acquisition method '{}':\n{}".format(
                method,
                "\n".join([f"- {row['Item Name']} (${row['Total Price']:,.2f})" 
                        for _, row in transactions.head(10).iterrows()])
            )  # Properly closed parentheses
        else:
            return f"⚠️ No transactions found with sub-acquisition method '{method}'"

    # ===== ITEM CLASSIFICATION =====
    if "segment and family classification" in question:
        item = re.search(r"the (.+?)\??$", question).group(1).strip().upper()
        # Handle NA values properly
        classification = df[df['Item Name'].notna() & 
                            df['Item Name'].str.upper().str.contains(item)]
        if not classification.empty:
            row = classification.iloc[0]
            return (f"🏷️ Classification for {item}:\n"
                    f"- Segment: {row.get('Segment Title', 'N/A')}\n"
                    f"- Family: {row.get('Family Title', 'N/A')}")
        else:
            return f"⚠️ No classification found for item {item}"

    # ===== ITEM QUANTITY BY CODE =====
    elif "how many items of" in question and "bought in" in question:
        match = re.search(r"items of (\w+) (?:bought|purchased) in (\d{4})", question)
        if match:
            item_code = match.group(1)
            year = int(match.group(2))
            # Search in both Classification Codes and Normalized UNSPSC
            mask = ((df['Classification Codes'].astype(str).str.contains(item_code)) |
                    (df['Normalized UNSPSC'].astype(str).str.contains(item_code))) & \
                    (df['Year'] == year)
            total = df[mask]['Quantity'].sum()
            return f"📦 Total quantity of items with code {item_code} in {year}: {int(total)}"

    # ===== TOP SUPPLIERS BY SPEND =====
    elif "top three suppliers based on total spend" in question:
        year_match = re.search(r"in (\d{4})", question)
        if year_match:
            year = int(year_match.group(1))
            top_suppliers = df[df['Year'] == year].groupby('Supplier Name')['Total Price']\
                        .sum().nlargest(3)
            return "🏆 Top 3 suppliers in {}:\n{}".format(
                year,
                "\n".join([f"- {supplier}: ${amt:,.2f}" 
                        for supplier, amt in top_suppliers.items()])
            )  # Fixed indentation
        else:
            top_suppliers = df.groupby('Supplier Name')['Total Price']\
                        .sum().nlargest(3)
            return "🏆 Top 3 suppliers overall:\n{}".format(
                "\n".join([f"- {supplier}: ${amt:,.2f}" 
                        for supplier, amt in top_suppliers.items()])
            )  # Fixed indentation

    # ===== DEPARTMENT SPENDING =====
    elif "total spend for the department" in question and "fiscal year" in question:
        dept_match = re.search(r"department (.+?) in fiscal year (\d{4})", question)
        if dept_match:
            dept = dept_match.group(1).strip()
            year = int(dept_match.group(2))
            # Handle department name variations
            dept_mask = df['Department Name'].str.contains(dept, case=False, na=False)
            year_mask = df['Fiscal Year'].str.contains(str(year), na=False)
            total = df[dept_mask & year_mask]['Total Price'].sum()
            return f"🏛️ Total spend for {dept.title()} in FY{year}: ${total:,.2f}"

    # ===== PURCHASES BY LOCATION =====
    elif "purchases linked to location" in question:
        loc_match = re.search(r"location (\d+)", question)
        if loc_match:
            zip_code = loc_match.group(1)
            purchases = df[df['Location'].astype(str).str.contains(zip_code)]
            if not purchases.empty:
                return "📍 Purchases for location {}:\n{}".format(
                    zip_code,
                    "\n".join([f"- {row['Item Name']} (${row['Total Price']:,.2f})" 
                            for _, row in purchases.head(10).iterrows()])
                )  # Fixed indentation
            else:
                return f"⚠️ No purchases found for location {zip_code}"

    # ===== SPENDING BY SUPPLIER CODE =====
    elif "total price for all purchases under supplier code" in question:
        code_match = re.search(r"supplier code (\d+)", question)
        if code_match:
            code = code_match.group(1)
            total = df[df['Supplier Code'].astype(str) == code]['Total Price'].sum()
            return f"🏢 Total spend for supplier code {code}: ${total:,.2f}"

     # ===== ITEMS BY ACQUISITION TYPE =====
    if "list all items purchased under the acquisition type" in question:
        acq_type = question.split("acquisition type")[1].strip()
        items = df[df['Acquisition Type'].str.contains(acq_type, case=False)][['Item Name', 'Item Description']].drop_duplicates()
        if not items.empty:
            return "📋 Items purchased under {}:\n{}".format(
                acq_type,
                "\n".join([f"- {row['Item Name']} ({row['Item Description']})" 
                          for _, row in items.iterrows()][:100])) # Limit to 100 items
        else:
            return f"⚠️ No items found under acquisition type {acq_type}"

    # ===== PURCHASES BY SUPPLIER QUALIFICATION =====
    if "purchases from suppliers with the qualification" in question:
        quals = question.split("qualification")[1].strip().upper()
        suppliers = df[df['Supplier Qualifications'].str.contains(quals, na=False)]
        if not suppliers.empty:
            return "🏢 Purchases from suppliers with {} qualification:\n{}".format(
                quals,
                "\n".join([f"- {row['Supplier Name'].title()} ({row['Item Name']})" 
                          for _, row in suppliers.iterrows()][:100]))  # Limit to 100
        else:
            return f"⚠️ No purchases found from suppliers with {quals} qualification"

    # ===== QUANTITY AND UNIT PRICE FOR SPECIFIC PO =====
    if "quantity and unit price for the item" in question and "purchase order" in question:
        item = re.search(r"item (.+?) in", question).group(1).upper()
        po = re.search(r"purchase order (.+?)$", question).group(1).strip()
        
        purchase = df[(df['Purchase Order Number'].astype(str).str.contains(po)) & 
                    (df['Item Name'].astype(str).str.contains(item, case=False))]
        
        if not purchase.empty:
            row = purchase.iloc[0]
            return f"📊 For item {item} in PO {po}:\n- Quantity: {row['Quantity']}\n- Unit Price: {format_currency(row['Unit Price'])}"
        else:
            return f"⚠️ No matching purchase found for item {item} in PO {po}"

    # ===== COUNT OF PURCHASES BY ACQUISITION METHOD =====
    if "how many purchases were made using the acquisition method" in question:
        method = question.split("acquisition method")[1].strip()
        count = df[df['Acquisition Method'].str.contains(method, case=False)].shape[0]
        return f"📦 Number of purchases using {method}: **{count}**"

    # ===== SEGMENT/FAMILY CLASSIFICATION =====
    if "segment and family classification does the" in question:
        item = question.split("the")[1].strip().upper()
        classification = df[df['Item Name'].str.contains(item, case=False)]
        if not classification.empty:
            row = classification.iloc[0]
            return f"🏷️ Classification for {item}:\n- Segment: {row.get('Segment Title', 'N/A')}\n- Family: {row.get('Family Title', 'N/A')}"
        else:
            return f"⚠️ No classification found for item {item}"

    # ===== TOTAL SPEND BY SUPPLIER IN FISCAL YEAR =====
    fiscal_year_match = re.search(r"total spend by (.+?) in (?:the )?fiscal year (\d{4})", question)
    if fiscal_year_match:
        supplier = fiscal_year_match.group(1).strip()
        year = int(fiscal_year_match.group(2))
        
        supplier_mask = df['Supplier Name'].str.lower().str.contains(supplier.lower())
        fiscal_year_mask = df['Fiscal Year'].str.contains(str(year))
        
        matched = df[supplier_mask & fiscal_year_mask]
        if not matched.empty:
            total = matched['Total Price'].sum()
            return f"💸 Total spend by {supplier.title()} in FY{year}: **${total:,.2f}**"
        else:
            return f"⚠️ No spending found for {supplier.title()} in FY{year}"

    # ===== CALCARD SPENDING IN FISCAL YEAR =====
    if "calcard" in question and "fiscal year" in question:
        year = int(re.search(r"fiscal year (\d{4})", question).group(1))
        calcard_mask = (df['CalCard'].str.upper() == "YES") & (df['Fiscal Year'].str.contains(str(year)))
        total = df[calcard_mask]['Total Price'].sum()
        return f"💳 Total CalCard spending in FY{year}: **${total:,.2f}**"

    # ===== ITEMS BY SUPPLIER AND LPA NUMBER =====
    lpa_match = re.search(r"how many items? (?:were|was) purchased from (.+?) using lpa number ([A-Za-z0-9\-]+)", question)
    if lpa_match:
        supplier = lpa_match.group(1).strip()
        lpa_num = lpa_match.group(2).strip()
        
        supplier_mask = df['Supplier Name'].str.lower().str.contains(supplier.lower())
        lpa_mask = df['LPA Number'].astype(str).str.lower() == lpa_num.lower()
        
        count = df[supplier_mask & lpa_mask].shape[0]
        return f"📦 Items purchased from {supplier.title()} under LPA {lpa_num}: **{count}**"

    # ===== ACQUISITION METHODS USED IN YEAR =====
    if "acquisition methods" in question and ("used" in question or "for purchases" in question):
        year_match = re.search(r"(\d{4})", question)
        if year_match:
            year = int(year_match.group(1))
            methods = df[df['Year'] == year]['Acquisition Method'].value_counts()
            if not methods.empty:
                return "📝 Acquisition methods used in {}:\n{}".format(
                    year,
                    "\n".join([f"- {method}: {count}" for method, count in methods.items()])
                )
            else:
                return f"⚠️ No acquisition method data found for {year}"

    # ===== PURCHASE ORDER NUMBER FOR REQUISITION =====
    po_match = re.search(r"purchase order number (?:for|of) (?:the )?requisition (\w+)", question)
    if po_match:
        req_num = po_match.group(1).upper()
        po_number = df[df['Requisition Number'].astype(str).str.upper() == req_num]['Purchase Order Number'].iloc[0]
        return f"🔢 Purchase Order Number for Requisition {req_num}: **{po_number}**"
    

        # ===== ORDERS FROM SUPPLIER ACROSS MULTIPLE YEARS =====
    if "total orders from" in question and "and" in question:
        supplier_match = re.search(r"total orders from (.+?) (?:in|during) (.+)", question)
        if supplier_match:
            supplier_query = supplier_match.group(1).strip()
            years_str = supplier_match.group(2)
            years = [int(y) for y in re.findall(r"\d{4}", years_str)]
            
            if not years:
                return "⚠️ Please specify valid year(s)"
                
            supplier_mask = df['Supplier Name'].astype(str).str.lower().str.contains(supplier_query.lower())
            results = []
            
            for year in years:
                year_mask = df['Year'] == year
                count = df[supplier_mask & year_mask].shape[0]
                results.append(f"- {year}: {count} orders")
            
            return f"📦 Orders from {supplier_query.title()}:\n" + "\n".join(results)

    # ===== TOTAL QUANTITY OF ITEM PURCHASED =====
    quantity_match = re.search(r"(?:total|how many) quantity of (.+?) (?:purchased|bought|ordered) (?:in|during) (\d{4})", question)
    if not quantity_match:
        quantity_match = re.search(r"(?:total|how many) (.+?) (?:were|was) (?:purchased|bought|ordered) (?:in|during) (\d{4})", question)
    
    if quantity_match:
        item_query = quantity_match.group(1).strip()
        year = int(quantity_match.group(2))
        
        item_mask = (
            df['Item Name'].astype(str).str.lower().str.contains(item_query.lower()) |
            df['Item Description'].astype(str).str.lower().str.contains(item_query.lower())
        )
        year_mask = df['Year'] == year
        total_quantity = df[item_mask & year_mask]['Quantity'].sum()
        
        if not pd.isna(total_quantity):
            return f"📦 Total quantity of {item_query} purchased in {year}: **{int(total_quantity)}**"
        else:
            return f"⚠️ No quantity data found for {item_query} in {year}"
        
        # ===== COUNT OF ITEMS ORDERED IN YEAR =====
    if re.search(r"how many items? (?:were|was) ordered? in (\d{4})", question):
        year = int(re.search(r"\d{4}", question).group())
        count = df[df['Year'] == year]['Item Name'].count()
        return f"📦 Total items ordered in {year}: **{count}**"

    # ===== COUNT OF SPECIFIC ITEMS PURCHASED =====
    match = re.search(r"how many (.+?) (?:were|was) (?:purchased|bought|ordered) (?:in|during) (\d{4})", question)
    if match:
        item_query = match.group(1).strip()
        year = int(match.group(2))
        
        # Clean item names for better matching
        item_mask = (
            df['Item Name'].astype(str).str.lower().str.contains(item_query, na=False) |
            df['Item Description'].astype(str).str.lower().str.contains(item_query, na=False)
        )
        year_mask = df['Year'] == year
        count = df[item_mask & year_mask].shape[0]
        
        return f"📦 Number of {item_query} purchased in {year}: **{count}**"

    # ===== COUNT OF ORDERS FROM SUPPLIER IN YEAR =====
    match = re.search(r"(?:total|how many) orders? (?:from|of|for) (.+?) (?:in|during) (\d{4})", question)
    if match:
        supplier_query = match.group(1).strip()
        year = int(match.group(2))
        
        supplier_mask = df['Supplier Name'].astype(str).str.lower().str.contains(supplier_query, na=False)
        year_mask = df['Year'] == year
        count = df[supplier_mask & year_mask].shape[0]
        
        return f"📦 Orders from {supplier_query.title()} in {year}: **{count}**"
    
        # Total price of specific items in a year
    match = re.search(r"total (?:price|spend|spending|amount) (?:of|for) ([\w\s\-]+) (?:purchased|bought)? (?:in|for|during) (\d{4})", question)
    if match:
        item_query = match.group(1).strip().lower()
        year = int(match.group(2))
        
        # Clean item names for better matching
        df['Item_Clean'] = df['Item Name'].astype(str).str.lower().str.strip()
        
        # Find matching items
        matched = df[(df['Item_Clean'].str.contains(item_query, na=False)) & 
                  (df['Year'] == year)]
        
        if not matched.empty:
            total = matched['Total Price'].sum()
            example_item = matched['Item Name'].mode()[0]
            return f"💸 Total spending on {item_query} in {year}: **${total:,.2f}**\n(Example item: {example_item})"
        else:
            return f"⚠️ No spending found for '{item_query}' in {year}"

    # CalCard spending in a year
    match = re.search(r"(?:total|how much) (?:calcard|cal card) (?:spend|spending|amount) (?:in|for|during) (\d{4})", question)
    if not match:
        match = re.search(r"(?:what was|what's) (?:the)? (?:calcard|cal card) (?:spend|spending|amount) (?:in|for|during) (\d{4})", question)
    if match:
        year = int(match.group(1))
        calcard_spending = df[(df['CalCard'].str.upper() == "YES") & 
                            (df['Year'] == year)]['Total Price'].sum()
        return f"💳 Total CalCard spending in {year}: **${calcard_spending:,.2f}**"
        # Extract any alphanumeric order number from the question
    order_num_match = re.search(r'(?:requisition|req|purchase order|po)[\s\-]?(?:number|no|#)?[\s\-]*([a-z0-9\-\.]+)', question, re.IGNORECASE)
    
    if order_num_match:
        search_num = order_num_match.group(1).upper()
        
        # Create a mask that searches both columns with exact matches
        mask = ((df['Requisition Number'].astype(str).str.upper() == search_num) | (df['Purchase Order Number'].astype(str).str.upper() == search_num))
        
        # Also try partial matches if exact match not found
        if not mask.any():
            mask = (
                (df['Requisition Number'].astype(str).str.upper().str.contains(search_num)) |
                (df['Purchase Order Number'].astype(str).str.upper().str.contains(search_num)
            ))
        
        matching_orders = df[mask]
        
        if not matching_orders.empty:
            # Find the best match (prioritize exact matches)
            if (df['Requisition Number'].astype(str).str.upper() == search_num).any():
                order = df[df['Requisition Number'].astype(str).str.upper() == search_num].iloc[0]
            elif (df['Purchase Order Number'].astype(str).str.upper() == search_num).any():
                order = df[df['Purchase Order Number'].astype(str).str.upper() == search_num].iloc[0]
            else:
                order = matching_orders.iloc[0]
            
            return format_order_details(order)
        else:
            return f"⚠️ No order found with number {search_num}"

    # Most expensive item
    if "most expensive" in question and ("item" in question or "purchase" in question):
        if 'Total Price' in df.columns and 'Item Name' in df.columns:
            most_expensive = df.loc[df['Total Price'].idxmax()]
            return (f"💎 Most expensive item purchased: **{most_expensive['Item Name']}**\n"
                   f"- Price: **{format_currency(most_expensive['Total Price'])}**\n"
                   f"- Supplier: {most_expensive['Supplier Name'].title()}\n"
                   f"- Date: {most_expensive['Purchase Date'].strftime('%m/%d/%Y') if pd.notna(most_expensive['Purchase Date']) else 'N/A'}")

    # Date range queries
    match = re.search(r"orders? between (\w+)\s+(\d{4}) and (\w+)\s+(\d{4})", question)
    if match:
        start_month, start_year, end_month, end_year = match.groups()
        start_date = pd.to_datetime(f"{start_month} 1, {start_year}")
        end_date = pd.to_datetime(f"{end_month} 1, {end_year}") + pd.offsets.MonthEnd(1)
        
        mask = (df['Purchase Date'] >= start_date) & (df['Purchase Date'] <= end_date)
        result = df[mask]
        return f"📅 Orders between {start_date.strftime('%b %Y')} and {end_date.strftime('%b %Y')}: **{len(result)}**"

    # Suppliers by ZIP code
    match = re.search(r"(?:list|show|all) suppliers? (?:from|in|with) zip(?: code)?\s*(\d{5})", question)
    if match:
        zip_code = match.group(1)
        suppliers = df[df['Supplier Zip Code'].astype(str).str.contains(zip_code)]['Supplier Name'].unique()
        if len(suppliers) > 0:
            return f"🏢 Suppliers from ZIP {zip_code}:\n\n" + "\n".join([f"- {str(s).title()}" for s in suppliers[:50]]) + \
                  f"\n\n(Showing {min(50, len(suppliers))} of {len(suppliers)} total suppliers)"
        else:
            return f"⚠️ No suppliers found in ZIP code {zip_code}"

    # Suppliers with qualifications
    match = re.search(r"suppliers? with ([\w\-]+) qualification", question)
    if match:
        qual = match.group(1).upper()
        suppliers = df[df['Supplier Qualifications'].str.contains(qual, na=False)]['Supplier Name'].unique()
        if len(suppliers) > 0:
            return f"🏢 Suppliers with {qual} qualification:\n\n" + "\n".join([f"- {str(s).title()}" for s in suppliers[:50]]) + \
                  f"\n\n(Showing {min(50, len(suppliers))} of {len(suppliers)} total suppliers)"
        else:
            return f"⚠️ No suppliers found with {qual} qualification"

    # Most common delivery location
    if "most common delivery location" in question or "location with most orders" in question:
        if 'Location' in df.columns:
            top_location = df['Location'].value_counts().idxmax()
            count = df['Location'].value_counts().max()
            return f"📍 Most common delivery location: **{top_location}** ({count} orders)"

    # ===== NEW QUESTION PATTERNS ADDED =====
    
    # Total price of [item] in [year]?
    match = re.search(r"total (?:price|spend|spending|amount) (?:of|for) ([\w\s\-]+) (?:in|for|during) (\d{4})", question)
    if not match:
        match = re.search(r"how much (?:was|did) (?:we|they) spend on ([\w\s\-]+) (?:in|for|during) (\d{4})", question)
    if match:
        item_query = match.group(1).strip().lower()
        year = int(match.group(2))
        matched = df[(df['Item Name'].str.lower().str.contains(item_query, na=False)) & 
                    (df['Year'] == year)]
        total = matched['Total Price'].sum()
        if not matched.empty:
            actual_name = matched['Item Name'].mode()[0]
            return f"💸 Total spending on {actual_name} in {year}: **${total:,.2f}**"
        else:
            return f"⚠️ No spending found for '{item_query}' in {year}"

    # List of suppliers?
    if re.search(r"(?:list|show|name|all|give me) (?:the|of|all)? ?suppliers?", question):
        if 'Supplier Name' in df.columns:
            suppliers = df['Supplier Name'].str.title().unique()
            return "🏢 List of Suppliers:\n\n" + "\n".join([f"- {supplier}" for supplier in suppliers[:10]]) + \
                  f"\n\n(Showing 10 of {len(suppliers)} total suppliers)"
        else:
            return "⚠️ No 'Supplier Name' column in data."

    # Total orders of [supplier]?
    match = re.search(r"total orders? (?:of|from|by|for) ([\w\s\-]+)", question)
    if match:
        supplier_query = match.group(1).strip().lower()
        matched = df[df['Supplier Name'].str.lower().str.contains(supplier_query, na=False)]
        return f"📦 Total orders from {supplier_query.title()}: **{len(matched)}**"

    # Total orders of [supplier] in [year]?
    match = re.search(r"total orders? (?:of|from|by|for) ([\w\s\-]+) (?:in|for|during) (\d{4})", question)
    if match:
        supplier_query = match.group(1).strip().lower()
        year = int(match.group(2))
        matched = df[(df['Supplier Name'].str.lower().str.contains(supplier_query, na=False)) & 
                    (df['Year'] == year)]
        return f"📦 Total orders from {supplier_query.title()} in {year}: **{len(matched)}**"

    # ===== ORIGINAL QUESTION HANDLING (KEPT AS IS) =====
    
    # Specific date: July 15, 2014
    match = re.search(r"(?:on|made on|placed on|received on)? ?(\w+)\s+(\d{1,2}),?\s+(\d{4})", question)
    if match:
        month_name, day, year = match.group(1), int(match.group(2)), int(match.group(3))
        month = month_str_to_number(month_name)
        if month:
            result = df[(df['Year'] == year) & (df['Month'] == month) & (df['Purchase Date'].dt.day == day)]
            return f"📅 Total orders on {month_name.capitalize()} {day}, {year}: **{len(result)}**"

    # Month + Year
    match = re.search(r"(?:in|for|from) (\w+) (\d{4})", question)
    if match:
        month_name, year = match.group(1), int(match.group(2))
        month = month_str_to_number(month_name)
        if month:
            result = df[(df['Year'] == year) & (df['Month'] == month)]
            return f"📦 Total orders in {month_name.capitalize()} {year}: **{len(result)}**"

    # Multiple years
    years = re.findall(r"\b(20\d{2})\b", question)
    if "order" in question and len(years) >= 2:
        summary = []
        for y in years:
            y = int(y)
            count = len(df[df['Year'] == y])
            summary.append(f"📦 {y}: {count} orders")
        return "📊 Total orders by year:\n\n" + "\n".join(summary)

    # Single year
    match = re.search(r"(?:how many|number of)? ?orders (?:were )?(?:placed|made|received)? ?(?:in|during|for)? ?(\d{4})", question)
    if match:
        year = int(match.group(1))
        result = df[df['Year'] == year]
        return f"📦 Total orders in {year}: **{len(result)}**"
    
    # Quarter with highest spending in a specific year
    match = re.search(r"quarter.*highest.*(?:in|for)? ?(\d{4})", question)
    if match:
        year = int(match.group(1))
        df_year = df[df['Year'] == year]
        if df_year.empty:
            return f"⚠️ No data for year {year}."
        spending = df_year.groupby('Quarter')['Total Price'].sum()
        if spending.empty:
            return f"⚠️ No spending data for year {year}."
        max_q = spending.idxmax()
        return f"💰 Quarter with highest spending in {year}: **Q{max_q} (${spending[max_q]:,.2f})**"

    # Orders from suppliers in ZIP [zip]
    match = re.search(r"orders? from suppliers? in zip (\d{5})", question)
    if match:
        zip_query = match.group(1)
        col = 'Supplier Zip Code'
        if col in df.columns:
            matched = df[df[col].astype(str).str.contains(zip_query, na=False)]
            return f"📦 Orders from suppliers in ZIP {zip_query}: **{len(matched)}**"
        else:
            return f"⚠️ No supplier ZIP column found in data."

    # Orders delivered to [zip]
    match = re.search(r"orders? delivered to (\d{5})", question)
    if match:
        zip_query = match.group(1)
        col = 'Location'
        if col in df.columns:
            matched = df[df[col].astype(str).str.contains(zip_query, na=False)]
            return f"📦 Orders delivered to {zip_query}: **{len(matched)}**"
        else:
            return f"⚠️ No delivery/location ZIP column found in data."

    # Orders with classification code [code]?
    match = re.search(r"classification code (\d+)", question)
    if match:
        code_query = match.group(1)
        col = 'Classification Codes'
        if col in df.columns:
            matched = df[df[col].astype(str).str.contains(code_query, na=False)]
            return f"📦 Orders with classification code {code_query}: **{len(matched)}**"
        else:
            return f"⚠️ No classification code column found in data."

    # Orders under [category] category?
    match = re.search(r"(orders|purchases) (under|in|from|for) (.+?) (category|item|group)?\??$", question)
    if match:
        keyword = match.group(3).strip().lower()
        category_cols = ['Commodity Title', 'Class Title', 'Family Title', 'Segment Title']
        found = False
        total_matches = pd.DataFrame()
        for col in category_cols:
            if col in df.columns:
                matches = df[df[col].str.lower().str.contains(keyword, na=False)]
                if not matches.empty:
                    total_matches = pd.concat([total_matches, matches])
                    found = True
        total_matches = total_matches.drop_duplicates()
        count = len(total_matches)
        if found and count > 0:
            return f"📦 Total orders under '{keyword}' category: **{count}**"
        else:
            return f"❌ No orders found under the category '{keyword}'."

    # How many orders did [supplier] make?
    match = re.search(r"how many orders did ([\w\s&\-\.]+) (make|place|have|do)\??", question)
    if match:
        supplier_query = match.group(1).strip().lower()
        matched = df[df['Supplier Name'].str.contains(supplier_query, na=False)]
        return f"📦 {supplier_query.title()} made **{len(matched)}** orders."

    # Total spend by [supplier]
    match = re.search(r"total (spend|spending|expenditure|amount) by ([\w\s&\-\.]+)", question)
    if match:
        supplier_query = match.group(2).strip().lower()
        matched = df[df['Supplier Name'].str.contains(supplier_query, na=False)]
        total = matched['Total Price'].sum()
        if not matched.empty:
            actual_name = matched['Supplier Name'].mode()[0]
            return f"💸 Total spend by {actual_name}: **${total:,.2f}**"
        else:
            return f"⚠️ No spending found for supplier '{supplier_query}'"

    # --- Orders by Acquisition Method or Type ---
    match = re.search(r"orders (?:using|used|with) ([\w\s\/\-]+)", question)
    if match:
        keyword = match.group(1).strip().lower()
        keyword_cleaned = re.sub(r"(method|type|acquisition)", "", keyword).strip()
        result_method = df[df['Acquisition Method'].str.lower().str.contains(keyword_cleaned, na=False)]
        result_type = df[df['Acquisition Type'].str.lower().str.contains(keyword_cleaned, na=False)]
        total = len(result_method) + len(result_type)
        if total > 0:
            return f"⚙️ Total orders using **{match.group(1).strip()}**: **{total}**"
        else:
            return f"⚠️ No orders found using '{match.group(1).strip()}'"

    # ---------------- SPENDING & FINANCIALS ---------------- #
    match = re.search(r"(?:total|overall) spending in (\d{4})", question)
    if match:
        year = int(match.group(1))
        result = df[df['Year'] == year]
        total = result['Total Price'].sum()
        return f"💸 Total spending in {year}: **${total:,.2f}**"

    match = re.search(r"average monthly spending(?: in| of)? ([\d,\sand]+)", question)
    if match:
        year_text = match.group(1)
        years = [int(y) for y in re.findall(r'\d{4}', year_text)]
        result = df[df['Year'].isin(years)]
        if result.empty:
            return f"⚠️ No records found for year(s): {', '.join(map(str, years))}"
        response_lines = []
        for y in years:
            year_data = result[result['Year'] == y]
            avg = year_data['Total Price'].sum() / 12
            response_lines.append(f"📊 {y}: **${avg:,.2f}**")
        return "📈 Average Monthly Spending:\n\n" + "\n".join(response_lines)

    if "quarter with" in question and "highest" in question:
        spending = df.groupby('Quarter')['Total Price'].sum()
        max_q = spending.idxmax()
        return f"💰 Quarter with highest spending: **Q{max_q} (${spending[max_q]:,.2f})**"

    # Total spending on a supplier
    match = re.search(r"total spending on ([\w\s&\-\.]+)", question)
    if match:
        supplier_query = match.group(1).strip().lower()
        matched = df[df['Supplier Name'].str.contains(supplier_query, na=False)]
        total = matched['Total Price'].sum()
        if not matched.empty:
            actual_name = matched['Supplier Name'].mode()[0]
            return f"💸 Total spending on {actual_name}: **${total:,.2f}**"
        else:
            return f"⚠️ No spending found for supplier '{supplier_query}'"

    # ---------------- SUPPLIER & ITEM ANALYTICS ---------------- #
    if "most frequent" in question and "item" in question:
        if 'Item Name' in df.columns:
            items = df['Item Name'].value_counts().head(5)
            return "🛒 Top 5 most frequently purchased items:\n\n" + "\n".join(
                [f"- {item} ({count})" for item, count in items.items()]
            )

    if (
        ("supplier" in question and (
            "most orders" in question or
            "highest number of orders" in question or
            "largest number of orders" in question or
            "greatest number of orders" in question or
            "maximum number of orders" in question or
            "biggest number of orders" in question or
            "top number of orders" in question
        ))
        or re.search(r"name of (the )?supplier with (the )?(most|highest|largest|greatest|max(imum)?|biggest|top) number of orders", question)
        or re.search(r"which supplier had (the )?(most|highest|largest|greatest|max(imum)?|biggest|top) number of orders", question)
        or re.search(r"who (is|was) (the )?supplier with (the )?(most|highest|largest|greatest|max(imum)?|biggest|top) number of orders", question)
        or re.search(r"supplier with (the )?(most|highest|largest|greatest|max(imum)?|biggest|top) number of orders", question)
    ):
        if 'Supplier Name' in df.columns:
            top_supplier = df['Supplier Name'].value_counts().idxmax()
            count = df['Supplier Name'].value_counts().max()
            return f"🏢 Supplier with most orders: **{top_supplier}** ({count} orders)"
        else:
            return "⚠️ No 'Supplier Name' column in data."

    if "spending by supplier" in question:
        supplier_spending = df.groupby('Supplier Name')['Total Price'].sum().sort_values(ascending=False).head(5)
        return "🏢 Top 5 suppliers by total spending:\n\n" + "\n".join(
            [f"- {supplier}: ${amount:,.2f}" for supplier, amount in supplier_spending.items()]
        )

    # ---------------- CLASSIFICATIONS & CATEGORIES ---------------- #
    if "most common class" in question:
        if 'Class Title' in df.columns:
            top_class = df['Class Title'].value_counts().idxmax()
            count = df['Class Title'].value_counts().max()
            return f"📚 Most common class: **{top_class}** ({count} orders)"

    if "top categories" in question or "top segments" in question:
        if 'Segment Title' in df.columns:
            segments = df['Segment Title'].value_counts().head(5)
            return "📦 Top 5 segments:\n\n" + "\n".join(
                [f"- {seg} ({count})" for seg, count in segments.items()]
            )

    # ---------------- LOCATION-BASED ---------------- #
    if "location with most orders" in question:
        if 'Location' in df.columns:
            top_location = df['Location'].value_counts().idxmax()
            count = df['Location'].value_counts().max()
            return f"📍 Location with most orders: **{top_location}** ({count} orders)"

    if re.search(r"(what|which) (items|item) (were|was)? ?(bought|purchased|ordered)? ?(the )?most", question):
        if 'Item Name' in df.columns:
            items = df['Item Name'].value_counts().head(5)
            return "🛒 Top 5 most frequently bought items:\n\n" + "\n".join(
                [f"- {item} ({count})" for item, count in items.items()]
            )
        else:
            return "⚠️ No 'Item Name' column in data."

    match = re.search(r"how much (did we|was)? ?(spend|spent) on ([\w\s&\-\.]+)", question)
    if match:
        item_query = match.group(3).strip().lower()
        item_query = re.sub(r"'s$", "", item_query)
        item_query = re.sub(r"s$", "", item_query)
        item_query = item_query.strip()
        if 'Item Name' in df.columns:
            matched = df[df['Item Name'].str.lower().str.contains(item_query, na=False)]
            total = matched['Total Price'].sum()
            if not matched.empty:
                actual_name = matched['Item Name'].mode()[0]
                return f"💸 Total spending on {actual_name}: **${total:,.2f}**"
            else:
                return f"⚠️ No spending found for item '{item_query}'"
        else:
            return "⚠️ No 'Item Name' column in data."

    match = re.search(r"orders in (the )?([\w\s&\-\.]+) segment", question)
    if match:
        segment_query = match.group(2).strip().lower()
        if 'Segment Title' in df.columns:
            matched = df[df['Segment Title'].str.lower().str.contains(segment_query, na=False)]
            return f"📦 Orders in the '{segment_query}' segment: **{len(matched)}**"
        else:
            return "⚠️ No 'Segment Title' column in data."

    if (
        ("supplier" in question and ("most expensive" in question or "highest spending" in question or "most spending" in question or "greatest spending" in question or "largest spending" in question))
        or re.search(r"who (was|is) (the )?(most expensive|highest spending|most spending|greatest spending|largest spending) supplier", question)
    ):
        if 'Supplier Name' in df.columns:
            supplier_spending = df.groupby('Supplier Name')['Total Price'].sum()
            top_supplier = supplier_spending.idxmax()
            amount = supplier_spending.max()
            return f"💸 Most expensive supplier: **{top_supplier}** (${amount:,.2f})"
        else:
            return "⚠️ No 'Supplier Name' column in data."

    match = re.search(r"orders? (?:from|by|with|placed with|made by) ([\w\s&\-\.]+) in (\d{4})", question)
    if match:
        supplier_query = match.group(1).strip().lower()
        year = int(match.group(2))
        matched = df[(df['Supplier Name'].str.contains(supplier_query, na=False)) & (df['Year'] == year)]
        return f"📦 Orders from {supplier_query.title()} in {year}: **{len(matched)}**"

    match = re.search(r"how many orders (from|by|with|placed with|made by) ([\w\s&\-\.]+)\??", question)
    if match:
        supplier_query = match.group(2).strip().lower()
        matched = df[df['Supplier Name'].str.contains(supplier_query, na=False)]
        return f"📦 Orders from {supplier_query.title()}: **{len(matched)}**"

    match = re.search(r"(top|show|list|give me|tell me|what are|which are)? ?(\d+)? ?(most|top)? ?(bought|purchased|ordered)? ?items", question)
    if match:
        n = match.group(2)
        n = int(n) if n else 10
        if 'Item Name' in df.columns:
            items = df['Item Name'].value_counts().head(n)
            return f"🛒 Top {n} most bought items:\n\n" + "\n".join(
                [f"- {item} ({count})" for item, count in items.items()]
            )
        else:
            return "⚠️ No 'Item Name' column in data."
    
    # ---------------- FALLBACK ---------------- #
    suggestions = [
        "Try asking about orders in a specific year or quarter",
        "Ask about spending by supplier or department",
        "Query about most frequently purchased items",
        "Ask 'Which supplier had the most orders?'",
        "Try 'What was the total spending in 2014?'"
    ]
    
    return "❌ I didn't understand your question. Try one of these:\n\n- " + "\n- ".join(suggestions[:3])

# Debugging Chat Interface 
def chatbot_response(message, history):
    debug_print(f"User asked: {message}")
    try:
        return handle_question(message)
    except Exception as e:
        debug_print(f"Response error: {e}")
        return f"❌ Error: {str(e)}"

# ---- Launch App ----
if __name__ == "__main__":
    debug_print("Starting enhanced Gradio interface")

    colorful_theme = gr.themes.Base(
        primary_hue="purple",
        secondary_hue="teal",
        font=[gr.themes.GoogleFont("Poppins"), "sans-serif"],
        spacing_size="lg"
    )

    gradient_css = """
    .gradio-container {
        background: linear-gradient(135deg, #36d1c4 0%, #5b86e5 50%, #8f6ed5 100%);
    }
    /* User message bubble: white background, blue text */
    .message.user, .chat-message.user, .bubble.user, .wrap.user {
        background: #fff !important;
        border-radius: 16px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        padding: 12px 18px !important;
        margin: 8px 0 !important;
    }
    .message.user *, .chat-message.user *, .bubble.user *, .wrap.user * {
        color: #2563eb !important;
    }
    /* Bot message bubble: white background, purple text */
    .message.bot, .chat-message.bot, .bubble.bot, .wrap.bot {
        background: #fff !important;
        border-radius: 16px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        padding: 12px 18px !important;
        margin: 8px 0 !important;
    }
    .message.bot *, .chat-message.bot *, .bubble.bot *, .wrap.bot * {
        color: #8f6ed5 !important;
    }
    """

    demo = gr.ChatInterface(
        fn=chatbot_response,
        title="🧠 Procurement Assistant 💼",
        description="First question will take long to process, but subsequent questions will be faster. 🤖📊",
        examples=[
            "How many orders were placed in 2014?",
            "Average monthly spending of 2013 and 2014?",
            "What items were bought the most?",
            "Orders from Pitney Bowes in 2013?",
            "How many orders did Correctional Health Care Services make?",
            "Orders in the food segment?"
        ],
        theme=colorful_theme,
        type="messages",
        css=gradient_css
    )

    demo.launch(
        server_name="127.0.0.1",
        server_port=7862,
        share=False,
        show_error=True
    )