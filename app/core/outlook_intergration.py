from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import logger
# 
import win32com.client
import pythoncom
import datetime 
import pandas as pd
import json

from datetime import timedelta 
LAST_RUN_FILE = "last_run.txt"
import re
import time
from app.core.email_parsing import EmailParser
from app.core.categ import EmailRulesEngine

def extract_current_body(body_text):
# Common reply markers
        markers = [
            r"^From:.*",  # Outlook reply format
            r"^On .* wrote:",  # Gmail-style reply
            r"^-----Original Message-----",  # Outlook classic
            r"^Sent:.*",  # Sent timestamp
        ]
       
        # Combine markers into one regex
        pattern = re.compile("|".join(markers), re.MULTILINE)
       
        # Split at first marker
        split_body = pattern.split(body_text)
       
        # Return the top part (current message)
        return split_body[0].strip() if split_body else body_text.strip()

def outlook_job():
       
        print(f"[{datetime.datetime.now()}] Cron job executed.")
        items = []
        pythoncom.CoInitialize()
 
        try:
            outlook = win32com.client.Dispatch("Outlook.Application")
            namespace = outlook.GetNamespace("MAPI")
            print("Outlook COM object available")
        except pythoncom.com_error as e:
            print("COM error:", e)
        # namespace = self.namespace_sp
       
       
       
        print("Namespace:", namespace)
        inbox = namespace.GetDefaultFolder(6)
        # "6" refers to the inbox
        messages2 = inbox.Items
        print("Message:",messages2.count)

        try:
            with open(LAST_RUN_FILE, "r") as f:
                # last_run = datetime.datetime.fromisoformat(f.read().strip())
                last_run = datetime.datetime.now() - datetime.timedelta(hours = 10)
        except FileNotFoundError:
            print("file not found")
            last_run = datetime.datetime.now() - datetime.timedelta(hours=10)
        now = datetime.datetime.now()
        five_minutes_ago = now - datetime.timedelta(minutes=5)
        start_time_str = five_minutes_ago.strftime("'%m/%d/%Y %H:%M %p'")
        # date_format = '%m/%d/%Y %H:%M:%S'
        # '%m/%d/%Y %I:%M %p'
        # start_time_str = five_minutes_ago.strftime('%m/%d/%Y %H:%M %p')
        
#         filter_string = "[ReceivedTime] >= '" + start_time_str + "'"
#         messages = inbox.Items
#         restricted_messages = messages.Restrict(filter_string)

# # Iterate over the filtered messages
#         for message in restricted_messages:
#             print(f"Subject: {message.Subject}, Received: {message.ReceivedTime}")
        # Format(last_run, "ddddd h:nn AMPM")
        # sFilter = "[LastModificationTime] > '" & Format("11/10/25 3:30pm", "ddddd h:nn AMPM") & "'"
        print(last_run)
        restriction = f"[ReceivedTime] >= '{last_run.strftime('%d/%m/%Y %I:%M %p')}'"
        print(restriction)
        messages = inbox.Items
        messages.Sort("[ReceivedTime]", True)
        #messages = messages.Restrict(restriction)
        # messages = inbox.Items.Restrict(restriction)
        # time.sleep(60)
        #print("Message:",messages.count)
        #messages.Sort("[ReceivedTime]", True)
        #print("Last run:",last_run.strftime('%m/%d/%Y %H:%M:%S'))
        output=[]
        parser=EmailParser()
        engine=EmailRulesEngine()
        all_rows_data=[]
        for msg in messages:
            try:
                
 
                # Get SMTP email using PropertyAccessor
                prop_accessor = msg.PropertyAccessor
                smtp_address = prop_accessor.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x5D01001F")
                #print("smtp_address",smtp_address)
 
                if smtp_address=="ControlUp@bitzer.de" or smtp_address == "tnarchana@kpmg.com":
                    input1={'subject':msg.Subject,"body":msg.Body}
                    #current_body = extract_current_body(msg.Body)
                    email_dataset=parser.get_deduplication_fields({"sender_address":smtp_address,
                                    "content":msg.Body,
                                    "subject":msg.Subject,
                                    "received_time":msg.ReceivedTime.isoformat()
                                    })
                    result = engine.process_email(email_dataset)

                    output1={'priority':result['priority'],'trigger_name':email_dataset['trigger_name'],'type':result.get('action_type',None),'resource_name':result.get('resource_name',None)}
                    
                    row_data = {}
    
    # 🟢 NEW LOGIC: Convert the entire input dictionary to a JSON string
                    row_data["Input"] = json.dumps(input1)
    
    # 🟢 NEW LOGIC: Convert the entire output dictionary to a JSON string
                    row_data["Output"] = json.dumps(output1)
    
    # Append the completed row dictionary to the list
                    all_rows_data.append(row_data)
                    #print("Current Message Body:\n", current_body,msg.Subject)
                   
 
                    # print(print(f"New Mail -> Subject: {msg.Subject}, Body: {msg.Body} from {msg.SenderName} at {msg.ReceivedTime}"))
                # print("SMTP Email Address:", smtp_address)
 
                # print("Received Time:", msg.ReceivedTime)
                #print("-" * 50)
        
 
            except Exception as e:
                print("Error processing message:", e)
        df = pd.DataFrame(all_rows_data)
    
    # Write the DataFrame to an Excel file (using openpyxl engine)
        df.to_excel("output_filename.xlsx", index=False)
            
        with open(LAST_RUN_FILE, "w") as f:
            f.write(datetime.datetime.now().isoformat())
        pythoncom.CoUninitialize()
        return output

outlook_job()

