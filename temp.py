import pandas as pd
import numpy as np
import pymssql
import datetime
import io
import imaplib
import email
from email.header import decode_header
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import json

"""
This python is used to read the mailbox to check the latest platform data and then import to database
"""

"""
Edit this part to read the Database credential json for connecting the Database 
sample json file:
{
  "server": "bi-tosa-db-pro.database.windows.net",
  "user": "pyauto",
  "password": "r:wnFL-bg6M%"
}
"""
DB_Login = json.load(open(r"C:\Operations\Credentials\db_credential.json",'r'))


"""
Edit this part to connect to the email used to receive daily platform file data
e.g.
imap_server = 'outlook.office365.com'  # IMAP server for Outlook
email_address = 'your_email@outlook.com'
email_password = 'your_password'
"""

imap_server = "imap.qiye.aliyun.com"
email_address = "notifier.bot@xtech-service.com"
email_password = "1nBqcDjYCRB7zH9K"


"""
Foodpanda file column and Database table column
"""
foodpanda_invoice_col_list = {'Order ID':'order_id', 'shop_id':'shop_id', 'start_time_utc':'order_datetime_start', 'complete_time_utc':'order_datetime_complete',
                    'Delivery Type':'category', 'Subtotal':'products_gfv', 'Discount':'order_discounts', 'Voucher':'vendor_voucher', 'Commission':'total_commission',
                    'Balance':'order_balance', 'filename_date':'filename_date', 'create_on':'create_on', 'update_on':'update_on'}

"""
Deliveroo file column and Database table column
"""
deliveroo_invoice_col_list = {'Order number':'order_id', 'shop_id':'shop_id', 'start_time_utc':'order_datetime_start', 'complete_time_utc':'order_datetime_complete',
                    'Delivery Type':'category', 'Subtotal':'products_gfv', 'filename_date':'filename_date', 'create_on':'create_on', 'update_on':'update_on'}

"""
Keeta file column and Database table column
"""
keeta_col_list = {'Order Number': 'order_code', 'Order Serial Number':'order_serial_num', 'Restaurant ID': 'supplier_code', 'Status': 'reversal', 'Order start time':'order_datetime_start',
                    'Completion Time': 'order_datetime_complete', 'Cancellation Time': 'order_datetime_cancel', 'Transaction Type': 'category', 'Original item price': 'products_gfv', 
                    'Top-up to minimum': 'order_adj', 'Promo items-Campaign Subsidies': 'vendor_voucher', 'Commission': 'total_commission', 'Commission Rate': 'commission_percentage', 'Minimum Commission': 'min_commission', 'Discount-Campaign Subsidies': 'order_discounts',
                    'Keeta Activity Subsidies': 'platform_discounts', 'Customer pays platform service fee': 'platform_service_fee', 'Customer pays for delivery fee': 'platform_delivery_fee', 
                    'Customer payment amount': 'customer_paid_amt', 'Payable to Restaurant': 'order_balance', 'Billing Date': 'bank_in_date', 'Billing Cycle': 'bank_in_date_desc', 'Notes': 'order_remarks',
                    'Delivery fee offer-Campaign Subsidies': 'delivery_fee_discounts', 'Meal for one-Campaign Subsidies': 'pandabox_fee'}



def send_error_mail(job, resp_text):
    """
    Edit this part to connect to the email used to receive error email
    e.g.
    smtp = smtplib.SMTP('smtp.gmail.com', 587) # connect to gmail SMTP 
    smtp.login('send_email@gmail.com', 'sample_password') # login to gmail to send the error email
    to = ["receive_email@gmail.com"]    # the gmail to receive error email
    """
    smtp = smtplib.SMTP('smtp-mail.outlook.com', 587)
    smtp.ehlo
    smtp.starttls()
    smtp.login('noreply@tasteofasia.com.hk', 'KJk%8dy8')

    body = 'Auto run the following python job get error. \nError path: ' + job + "\nError message: " + resp_text

    msg = MIMEMultipart()
    msg['Subject'] = '[Auto_Message] TOA daily platform sales import Error'
    msg.attach(MIMEText(body))

    to = ["leo.chan@xtech-service.com","leon.huang@xtech-service.com","bin.zhang@xtech-service.com"]
    smtp.sendmail(from_addr="",
                to_addrs=to, msg=msg.as_string())
    smtp.quit()



def upload_file(url, file_content, filename):
    headers = {
        "x-ms-blob-type": "BlockBlob",  
        "Content-Type": "application/octet-stream",  
        "Content-Disposition": f'attachment; filename="{filename}"' 
    }

    try:
        response = requests.put(url, headers=headers, data=file_content.encode("utf-8"))

        # print(f"Status Code: {response.status_code}")
        # print(f"Response: {response.text}")

        if response.status_code in [200, 201, 204]:
            print("File uploaded successfully!")
        else:
            print("File upload failed!")

    except Exception as e:
        send_error_mail('Upload file error', f'filename={filename}')
        print(f"Error: {e}")
        



def connect_to_DB(database):

    return pymssql.connect(**DB_Login, database = database)


def cleaning_foodpanda_data(DB, df, db_col_list, file_date):
    """
    Edit Foodpanda data to Database format
    """
    try:
        df = df.filter(['Order ID', 'Restaurant ID', 'Delivery Type', 'Order status', 'Order received at', 'Accepted at', 'Estimated delivery time', 'Delivered at', 'Subtotal', 'Discount', 'Voucher', 'Is Payable'], axis=1)

        new_df = df[df["Order status"].str.contains("Cancelled") == False]
        new_df = new_df[new_df["Order status"].str.contains("plugins.reports.order_details_report.displayed_at_vendor") == False]
        new_df = new_df.copy()
        new_df.loc[:, 'Subtotal'] = new_df['Subtotal'].replace({r'\$': '', ',': ''}, regex=True)
        new_df.loc[:, 'Subtotal'] = pd.to_numeric(new_df['Subtotal'], errors='coerce')
        new_df.loc[:, 'Discount'] = new_df['Discount'].replace({r'\$': '', ',': ''}, regex=True)
        new_df.loc[:, 'Discount'] = pd.to_numeric(new_df['Discount'], errors='coerce')
        new_df.loc[:, 'Voucher'] = new_df['Voucher'].replace({r'\$': '', ',': ''}, regex=True)
        new_df.loc[:, 'Voucher'] = pd.to_numeric(new_df['Voucher'], errors='coerce')
        new_df.loc[:, 'Commission'] = round((new_df['Subtotal'] - new_df['Discount'] - new_df['Voucher']) * 0.22, 2)
        new_df.loc[:, 'Balance'] = round((new_df['Subtotal'] - new_df['Discount'] - new_df['Voucher'] - new_df['Commission']), 2)
        # new_df.loc[:, 'Commission'] = round((new_df['Subtotal'] - new_df['Discount'] - new_df['Voucher']) * 0.23, 2)
        # new_df.loc[:, 'Balance'] = round((new_df['Subtotal'] - new_df['Discount'] - new_df['Voucher']) * (1 - 0.23), 2)
        new_df.loc[:, 'Delivery Type'] = new_df['Delivery Type'].apply(lambda x: 'FOODPANDA' if 'delivery' in x else 'SELFPICKED')

        new_df.loc[:, 'start_time'] = new_df['Order received at'].fillna(new_df['Accepted at'])
        new_df.loc[:, 'start_time_utc'] = pd.to_datetime(new_df['start_time']) - datetime.timedelta(hours=8)
        new_df.loc[:, 'complete_time'] = new_df['Delivered at'].fillna(new_df['Estimated delivery time'])
        new_df.loc[:, 'complete_time_utc'] = pd.to_datetime(new_df['complete_time']) - datetime.timedelta(hours=8)

        new_df.loc[:, 'filename_date'] = file_date


        cursor = DB.cursor(as_dict=True)
        shop_codes = tuple(new_df['Restaurant ID'].unique())
        query = f"SELECT shop_foodpanda_code1, shop_id FROM shop_table WHERE shop_foodpanda_code1 IN {shop_codes} and not shop_status = 'N'"
        cursor.execute(query)
        result = cursor.fetchall()
        id_mapping = {row['shop_foodpanda_code1']: row['shop_id'] for row in result}

        new_df.loc[:, 'shop_id'] = new_df.loc[:, 'Restaurant ID'].map(id_mapping)

        shop_id_col = new_df.pop('shop_id')
        new_df.insert(1, 'shop_id', shop_id_col)

        new_df['create_on'] = datetime.datetime.now()
        new_df['update_on'] = datetime.datetime.now()

        new_df = new_df.drop(columns=['Restaurant ID', 'Estimated delivery time', 'start_time', 'complete_time', 'Delivered at', 'Order received at', 'Accepted at', 'Is Payable', 'Order status'], axis=1)
        new_df = new_df.replace(np.nan, None)
        new_df = new_df.rename(columns=db_col_list)


        return new_df

    except Exception as e:
        send_error_mail('cleaning_foodpanda_data', 'cleaning data error')
        print(e)


def cleaning_deliveroo_data(DB, df, db_col_list, file_date):
    """
    Edit Deliveroo data to Database format
    """
    try:
        df.columns = df.columns.str.strip()

        df = df.filter(['Restaurant name', 'Order number', 'Order status', 'Date submitted', 'Time submitted', 'Date delivered', 'Time delivered', 'Subtotal', 'Deliveroo commission'], axis=1)

        new_df = df[df["Order status"].str.contains("Completed") == True]
        new_df = new_df.copy()
        new_df['Subtotal'] = pd.to_numeric(new_df['Subtotal'], errors='coerce')
        new_df['shop_code'] = new_df['Restaurant name'].apply(lambda x: x.split('-')[1].strip() if '-' in x  else None)
        new_df['edited_shop_code'] = new_df['shop_code'].apply(lambda x: x[0:5].strip())
        new_df['order_datetime_start'] = pd.to_datetime(new_df['Date submitted'] + ' ' + new_df['Time submitted'])
        new_df['order_datetime_complete'] = pd.to_datetime(new_df['Date delivered'] + ' ' + new_df['Time delivered'])

        new_df['start_time_utc'] = pd.to_datetime(new_df['order_datetime_start']) - datetime.timedelta(hours=8)
        new_df['complete_time_utc'] = pd.to_datetime(new_df['order_datetime_complete']) - datetime.timedelta(hours=8)


        new_df.loc[:, 'order_balance'] = new_df['Subtotal']

        new_df.loc[:, 'order_discounts'] = 0
        new_df.loc[:, 'vendor_voucher'] = 0
        new_df.loc[:, 'total_commission'] = 0

        new_df.loc[:, 'filename_date'] = file_date



        cursor = DB.cursor(as_dict=True)
        shop_codes = tuple(new_df['edited_shop_code'].unique())
        query = f"SELECT shop_code, shop_code1, shop_id FROM shop_table WHERE (shop_code IN {shop_codes} or shop_code1 IN {shop_codes}) and not shop_status = 'N'"
        cursor.execute(query)
        result = cursor.fetchall()
        id_mapping = {}
        for row in result:
            if row['shop_code']:
                id_mapping[row['shop_code']] = row['shop_id']
            if row['shop_code1']:
                id_mapping[row['shop_code1']] = row['shop_id']

        new_df.loc[:, 'shop_id'] = new_df.loc[:, 'edited_shop_code'].map(id_mapping)

        shop_id_col = new_df.pop('shop_id')
        new_df.insert(1, 'shop_id', shop_id_col)

        new_df['create_on'] = datetime.datetime.now()
        new_df['update_on'] = datetime.datetime.now()

        new_df = new_df.drop(columns=['Restaurant name', 'Order status', 'Date submitted', 'Time submitted', 'Date delivered', 'Time delivered', 'Deliveroo commission', 'shop_code', 'edited_shop_code', 'order_datetime_start', 'order_datetime_complete'], axis=1)
        new_df = new_df.replace(np.nan, None)
        new_df = new_df.rename(columns=db_col_list)


        return new_df

    except Exception as e:
        send_error_mail('cleaning_deliveroo_data', 'cleaning data error')
        print(e)


def update_order_code(row):
    formatted_amount = f"{abs(row['order_balance']):08.2f}".replace('.', '')

    if row['order_balance'] < 0:
        return f"{row['order_code']}_{row['category']}_-{'{:08d}'.format(int(formatted_amount))}"
    else:
        return f"{row['order_code']}_{row['category']}__{'{:08d}'.format(int(formatted_amount))}"


def cleaning_keeta_data(DB, df, db_col_list, file_date):
    """
    Edit Keeta data to Database format
    """
    try:
        df.columns = df.columns.str.strip()

        df = df.drop(columns=['Brand Name', 'Brand ID', 'Restaurant Name', 'shop code'], axis=1)

        new_df = df
        new_df = new_df.copy()
        
        new_df = new_df.rename(columns=db_col_list)


        new_df['Campaign Subsidies'] = new_df['Campaign Subsidies'].fillna(0)
        new_df['vendor_voucher'] = new_df['vendor_voucher'].fillna(0)
        new_df['Price reduction-Campaign Subsidies'] = new_df['Price reduction-Campaign Subsidies'].fillna(0)
        new_df['Free delivery-Campaign Subsidies'] = new_df['Free delivery-Campaign Subsidies'].fillna(0)
        new_df['order_discounts'] = new_df['order_discounts'].fillna(0)
        new_df['delivery_fee_discounts'] = new_df['delivery_fee_discounts'].fillna(0)
        new_df['pandabox_fee'] = new_df['pandabox_fee'].fillna(0)
        new_df['Special campaign dishes-Campaign Subsidies'] = new_df['Special campaign dishes-Campaign Subsidies'].fillna(0)

        new_df = new_df.drop(new_df.filter(regex="Subsidies").columns, axis=1)
        new_df['order_datetime_start'] = pd.to_datetime(new_df['order_datetime_start'], format = '%d %b %Y at %H:%M:%S',errors = 'coerce')
        new_df['order_datetime_complete'] = pd.to_datetime(new_df['order_datetime_complete'], format = '%d %b %Y at %H:%M:%S',errors = 'coerce')
        new_df['order_datetime_cancel'] = pd.to_datetime(new_df['order_datetime_cancel'], format = '%d %b %Y at %H:%M:%S',errors = 'coerce')
        new_df['bank_in_date'] = pd.to_datetime(new_df['bank_in_date'], format='%d %b %Y')

        new_df['reversal'] = new_df['reversal'].apply(lambda x: 'X' if isinstance(x, str) and 'Cancelled' in x else None)
        new_df['commission_percentage'] = new_df['commission_percentage'].replace('%', '', regex=True).astype(float)

        new_df['order_datetime_start'] = pd.to_datetime(new_df['order_datetime_start']) - datetime.timedelta(hours=8)
        new_df['order_datetime_complete'] = pd.to_datetime(new_df['order_datetime_complete']) - datetime.timedelta(hours=8)
        new_df['order_datetime_cancel'] = pd.to_datetime(new_df['order_datetime_cancel']) - datetime.timedelta(hours=8)

        cursor = DB.cursor(as_dict=True)
        shop_codes = tuple(str(x) for x in new_df['supplier_code'].unique())

        query = f"SELECT shop_keeta_code as supplier_code, shop_id FROM shop_table WHERE shop_keeta_code IN {shop_codes} and not shop_status = 'N'"
        cursor.execute(query)
        result = cursor.fetchall()
        id_mapping = {row['supplier_code']: row['shop_id'] for row in result}

        new_df.loc[:, 'shop_id'] = new_df.loc[:, 'supplier_code'].astype(str).map(id_mapping)

        shop_id_col = new_df.pop('shop_id')
        new_df.insert(1, 'shop_id', shop_id_col)

        new_df['platform_type'] = 'keeta'
        new_df['vendor_code'] = None
        new_df['order_date'] = None
        new_df['invoice_number'] = None
        new_df['order_adj_rate'] = None
        new_df['data_from_file'] = file_date.replace('-', '') + '_keeta'
        new_df['filename_date'] = file_date



        new_df['create_on'] = datetime.datetime.now()
        new_df['update_on'] = datetime.datetime.now()


        new_df['order_code'] = new_df.apply(update_order_code, axis=1)
        

        category_keywords = ['Customer Compensation', 'Service Fee', 'Adjustment']
        df_b = new_df[new_df['category'].str.contains('|'.join(category_keywords), case=False, na=False)].copy()
        df_a = new_df[~new_df['category'].str.contains('|'.join(category_keywords), case=False, na=False)].copy()

        df_b['vendor_code'] = df_b.loc[:, 'supplier_code']
        df_b['order_discounts'] = None
        df_b['supplier_code'] = None
        df_b['invoicing_date'] = None
        df_b['quanatity'] = None
        df_b['unit_price'] = None
        df_b['net_total'] = None
        df_b['currency_key'] = None
        df_b['description'] = None


        df_a['order_adj'] =  df_a['order_adj'].fillna(0)
        df_a['platform_discounts'] =  df_a['platform_discounts'].fillna(0)
        df_a['platform_service_fee'] =  df_a['platform_service_fee'].fillna(0)
        df_a['platform_delivery_fee'] =  df_a['platform_delivery_fee'].fillna(0)
        df_a['delivery_fee_discounts'] =  df_a['delivery_fee_discounts'].fillna(0)
        df_a['pandabox_fee'] =  df_a['pandabox_fee'].fillna(0)
        df_a['customer_paid_amt'] =  df_a['customer_paid_amt'].fillna(0)


        return df_a, df_b


    except Exception as e:
        send_error_mail('cleaning_keeta_data', 'cleaning data error')
        print(e)
        return None, None


def merge_into_db(DB, target_table, df, unique_keys):
    """
    Merge new data into Database
    Insert or Update data, if unique_keys matched then Update, otherwise Insert
    """
    cursor = DB.cursor()
    cursor.execute(f'SELECT TOP 1 * FROM {target_table}')

    # Get column names excluding primary key (assumed first column)
    col_name = [i[0] for i in cursor.description][1:]


    # Ensure DataFrame columns match DB column names
    df = df.reindex(columns = col_name)

    # Wrap column names in brackets for SQL Server
    col_name_brackets = [f'[{col}]' for col in col_name]

    merge_query = f"""
    MERGE {target_table} AS target
    USING (VALUES ({', '.join(['%s'] * df.shape[1])}))
    AS source ({', '.join(col_name_brackets)})
    ON {' AND '.join([f'target.[{key}] = source.[{key}]' for key in unique_keys])}
    WHEN MATCHED THEN
        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in col_name if col not in unique_keys and col != 'create_on'])}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(col_name_brackets)})
        VALUES ({', '.join([f'source.{col}' for col in col_name_brackets])});
    """

    df = df.replace({np.nan: None})


    try:
        # print(merge_query)
        cursor.executemany(merge_query, (tuple(row) for row in df.itertuples(index=False)))
        DB.commit()
        print(f"{target_table} successfully inserted/updated")

    except Exception as e:
        send_error_mail('merge_into_db', f'insert {target_table} error')
        print(e)
        DB.rollback()

    finally:
        cursor.close()


def login_to_outlook():
    """Connect to the IMAP server and select the inbox."""
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(email_address, email_password)
    mail.select("inbox")  # Default folder to monitor
    return mail


def main():

    mail = login_to_outlook()

    last_email_id = None
    try:
        # while True:
        #     try:
                # Search for all emails
                status, messages = mail.search(None, "ALL")
                if status == "OK":
                    # Fetch the latest email
                    email_ids = messages[0].split()
                    latest_email_id = email_ids[-1]  # Get the last email ID
                    if latest_email_id != last_email_id:
                        status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
                        if status != "OK":
                            print("Failed to fetch the email!")

                        df_foodpanda = None
                        df_deliveroo = None
                        foodpanda_filename = None

                        file_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d')

                        # Parse the email
                        for response_part in msg_data:
                            if isinstance(response_part, tuple):
                                # Parse the raw email content
                                msg = email.message_from_bytes(response_part[1])

                                subject, encoding = decode_header(msg["Subject"])[0]
                                if isinstance(subject, bytes):
                                    # Decode bytes to a string
                                    subject = subject.decode(encoding if encoding else "utf-8")

                                if 'delivery platform' in subject and file_date in subject:
                                    print(f"Email Subject: {subject}")


                                    # Check for attachments
                                    print(msg.is_multipart())
                                    if msg.is_multipart():
                                        DB = connect_to_DB('c0217_sql-2025')
                                        for part in msg.walk():
                                            content_disposition = part.get("Content-Disposition", "")
                                            if "attachment" in content_disposition:
                                                # Get the filename
                                                filename = part.get_filename()
                                                file_data = part.get_payload(decode=True)
                                                dbytes,charset = decode_header(filename)[0]
                                                filename=dbytes.decode(charset)
                                                print(filename)

                                                


                                                if filename and filename.startswith("foodpanda"):
                                                # if filename and "foodpanda" in filename:
                                                    print("start foodpanda job")
                                                    print(filename)
                                                    csv_content = file_data.decode("utf-8")
                                                    # Insert to DB
                                                    df_foodpanda = pd.read_csv(io.StringIO(csv_content))
                                                    df_foodpanda = cleaning_foodpanda_data(DB, df_foodpanda, foodpanda_invoice_col_list, file_date)
                                                    merge_into_db(DB, 'foodpanda_invoice_table', df_foodpanda, ['order_id', 'shop_id', 'order_datetime_start'])
                                                    # Upload file to blob
                                                    # if foodpanda_filename is None:
                                                    #     foodpanda_filename = f'{file_date}_foodpanda.csv'
                                                    # else:
                                                    #     foodpanda_filename = f'{file_date}_2_foodpanda.csv'
                                                    # url = rf"https://sample.blob/{foodpanda_filename}?sv=sample"
                                                    # upload_file(url, csv_content, filename)


                                                # if filename and filename.startswith("deliveroo"):
                                                # # if filename and "deliveroo" in filename:
                                                #     print("start deliveroo job")
                                                #     print(filename)
                                                #     csv_content = file_data.decode("utf-8")
                                                #     # Insert to DB
                                                #     df_deliveroo = pd.read_csv(io.StringIO(csv_content))
                                                #     df_deliveroo = cleaning_deliveroo_data(DB, df_deliveroo, deliveroo_invoice_col_list, file_date)
                                                #     merge_into_db(DB, 'deliveroo_invoice_table', df_deliveroo, ['order_id', 'shop_id', 'order_datetime_start', 'filename_date'])
                                                #     # Upload file to blob
                                                #     deliveroo_filename = f'{file_date}_deliveroo.csv'
                                                #     url = rf"https://sample.blob/{deliveroo_filename}?sv=sample"
                                                #     upload_file(url, csv_content, filename)


                                                if filename and filename.startswith("keeta"):
                                                    df_keeta = pd.read_excel(io.BytesIO(file_data), engine="openpyxl", sheet_name='Order Summary', dtype={'Order Number': str})
                                                    df_keeta_a, df_keeta_b = cleaning_keeta_data(DB, df_keeta, keeta_col_list, file_date)
                                                    merge_into_db(DB, 'keeta_a_table', df_keeta_a, ['order_code', 'shop_id', 'order_datetime_start', 'filename_date'])
                                                    merge_into_db(DB, 'keeta_b_table', df_keeta_b, ['order_code', 'shop_id', 'order_datetime_start', 'filename_date'])


                                                last_email_id = latest_email_id

                                        DB.close()


            # except imaplib.IMAP4.abort:
            #     print("Connection lost. Reconnecting...")
            #     mail = login_to_outlook()

            # time.sleep(300000000)


    except KeyboardInterrupt:
        print("Monitoring stopped.")
    # finally:
    #     mail.logout()


if __name__ == '__main__':
    main()
