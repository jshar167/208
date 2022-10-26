import mysql.connector
import pandas as pd
import numpy as np
import time
from dateutil.rrule import rrule, MONTHLY
import datetime
import os

print("Reading customers name")

filter = pd.read_excel(r"customer_filter_name.xlsx", engine='openpyxl')



filter_name = tuple(set(filter['Customer_name']))
if len(filter['Customer_name'])==1:
     filter_name = '(%s)' % ', '.join(map(repr, tuple(set(filter['Customer_name']))))
else:
     filter_name = str(tuple(set(filter['Customer_name'])))

print("customers name are")
print("filter_name")


print("setting connection with MySQL")
print("must be connected to VPN for the connection")

try:
    connection = mysql.connector.connect(host='prod-rds-bi.xaapbuildings.com',
                                         user='sandeep.sharma',
                                         port = '3306',
                                         password='k3XqezAa')


    cursor = connection.cursor()
    
    df1 = pd.DataFrame(columns = ['customer_name', 'building_name','id','device_type', 
                          'summary_status'])

    a = datetime.date(2019, 1, 1)
    b = datetime.date.today()
    
    
    ls = []
    for dt in rrule(MONTHLY, interval = 24, dtstart=a, until=b):
        ls.append(dt.strftime("%Y-%m-%d"))
    ls.append(b.strftime("%Y-%m-%d"))

    for i in range (len (ls)-1):
        print("fetching deficiency details- part " +str(i+1))
    
        Query1 = """select distinct c.name as customer_name, 
            b.name as building_name, 
            f.id ,
            f.device_type,
            f.summary_status
            from bi.deficiency f, bi.customer c, bi.building b
            where f.tenant_id = c.id
            and f.building_id = b.id
            and c.name in {}
            and f.last_updated_at >= '{}' and f.last_updated_at < '{}' order by id;""".format(filter_name,ls[i], ls[i+1])
        
        
        cursor.execute(Query1)
        table_rows = cursor.fetchall()
        
        df_sub = pd.DataFrame(table_rows)
        df_sub.columns = ['customer_name', 'building_name','id','device_type', 
                          'summary_status']
        
        df1 = pd.concat([df1, df_sub], ignore_index= True)
        
        print(str(i+1) + " is done")
    
       
except mysql.connector.Error as error:
    print("Failed to connect MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")




df1['device_type'] = df1['device_type'].str.lower().replace(".","").str.replace(" ","").str.replace("/","").replace("firedept.connection","firedeptconnection")



deficiency_pivot = pd.pivot_table(data=df1, index=['customer_name', 'building_name','device_type'], 
                            values=['id'],
                            aggfunc='count', 
                            margins=['customer_name', 'building_name','device_type'],
                            margins_name='Grand Total',
                            fill_value=0)


device_type_filter = str(tuple(df1['device_type'].unique()))




try:
    connection = mysql.connector.connect(host='prod-rds-bi.xaapbuildings.com',
                                         user='sandeep.sharma',
                                         port = '3306',
                                         password='k3XqezAa')


    cursor = connection.cursor()
    
    df2 = pd.DataFrame(columns = ['customer_name', 'building_name','id','device_type'])

    a = datetime.date(2019, 1, 1)
    b = datetime.date.today()
    
    
    ls = []
    for dt in rrule(MONTHLY, interval = 48, dtstart=a, until=b):
        ls.append(dt.strftime("%Y-%m-%d"))
    ls.append(b.strftime("%Y-%m-%d"))

    for i in range (len (ls)-1):
        print("fetching details for device- part " +str(i+1))
    
        Query2 = """select distinct c.name as customer_name, 
            b.name as building_name, 
            d.id,
            d.type as device_type
            from bi.device d, bi.customer c, bi.building b, bi.inspection i
            where i.tenant_id = c.id
            and i.building_id = b.id
            and d.inspection_id = i.id
            and c.name in {}
            and i.end_date >= '{}' and i.end_date < '{}' order by id;""".format(filter_name, ls[i], ls[i+1])
        
        
        cursor.execute(Query2)
        table_rows = cursor.fetchall()
        df_sub = pd.DataFrame(table_rows)
        df_sub.columns = ['customer_name', 'building_name','id','device_type']
        
        
        df2 = pd.concat([df2, df_sub], ignore_index= True)
        
        print(str(i+1) + " is done")
    
       
except mysql.connector.Error as error:
    print("Failed to connect MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")




df2['device_type'] = df2['device_type'].map(lambda x: x.rstrip('s'))
df2['device_type'] = df2['device_type'].str.replace("batterie","battery").str.replace("waterflowswitche","waterflowswitch").replace("tamperswitche","tamperswitch")



device_pivot = pd.pivot_table(data=df2, index=['customer_name', 'building_name','device_type'], 
                            values=['id'],
                            aggfunc='count', 
                            margins=['customer_name', 'building_name','device_type'],
                            margins_name='Grand Total',
                            fill_value=0)




new_df = pd.merge(deficiency_pivot, device_pivot,  how='left', left_on=['customer_name','building_name','device_type' ], right_on = ['customer_name','building_name','device_type' ])



new_df["Failure_rate"] = round((new_df.loc[: ,('id_x')] / new_df.loc[: ,('id_y')])*100,2).astype(str) + '%'
new_df.rename(columns = {'id_x':'failure_count'}, inplace = True)
new_df.rename(columns = {'id_y':'device_count'}, inplace = True)


print("Report is being saved")

def save_excel_sheet(df, path, sheet_name, index=False):
    # Create file if it does not exist
    if not os.path.exists(path):
        df.to_excel(path, sheet_name=sheet_name, index=index)

    # Otherwise, add a sheet. Overwrite if there exists one with the same name.
    else:
        with pd.ExcelWriter(path, engine='openpyxl', if_sheet_exists='replace', mode='a') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)

            
path = r"final_209.xlsx"
df = df1
sheet_name = "deficiency_Raw"

save_excel_sheet(df, path, sheet_name, index = False)


df = df2
sheet_name = "device_Raw"

save_excel_sheet(df, path, sheet_name, index = False)


df = new_df
sheet_name = "failure_rate_pivot"

save_excel_sheet(df, path, sheet_name, index = True)

print("output is saved at " +path +" with sheet name " +sheet_name)



