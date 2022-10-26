
from pyArango.connection import *
import pandas as pd
import os

print("Reading customers name" )
filter = pd.read_excel(r"customer_filter_name_ArangoDB.xlsx")


l = list(filter.Customer_name)
print("name of customers--" )
print(l)



print("setting up connection with ArangoDB" )
print("make sure you are connceted to VPN" )

conn = Connection(arangoURL = 'http://prod-arangodb-tenant.xaapbuildings.com:8529', 
                  username="xaap_prod_ro", password="30blJnCmGRm@uYGi5n")



aql1 = '''FOR t in tenants
FILTER t.name in '''

aql2 = str(l)

aql3 = '''  SORT t.name

FOR device in v_devices
FILTER t._key == device.tenantKey

//customer_id: t._key,
//customer_name: t.name

// optionally, add a specific tenant key here
// FILTER t._key == "116698886"
// optionally, set a limit to reduce load when playing around with this
// LIMIT 0, 5000

// gather the details into an intermediate document
LET sub = MERGE(
   
    // get the customer details or 'unknown' if there is a hanging edge in the graph
    {
        customer_id: t._key,
        customer_name: t.name
    },
   
    // get the parent device details or 'unknown' if there is no parent device
    FIRST(FOR v IN 1..1 INBOUND device._id owns
        FILTER v.type != 'buildings'
        RETURN {
            parent_device_id: v.instanceId,
            parent_device_manufacturer: v.manufacturer,
            parent_device_make: v.make,
            parent_device_model: v.model,
            parent_device_type: v.type,
            parent_device_serial: v.serial}
    ) || {
        'parent_device_id': 'unknown',
        'parent_device_manufacturer': 'unknown',
        'parent_device_make': 'unknown',
        'parent_device_model': 'unknown',
        'parent_device_type': 'unknown',
        'parent_device_serial': 'unknown'
    },
       
    // get the building details or 'unknown' if there is a hanging edge in the graph
    FIRST(FOR v IN 1..2 INBOUND device._id owns
        FILTER v.type == 'buildings'
        RETURN {
            building_id: v._key,
            building_name: v.name
        }
    ) || {
        'building_id': 'unknown',
        'building_name': 'unknown'
    },
   
    // get the device details
    {
        device_id: device.instanceId,
        device_type: device.type,
        device_manufacturer: device.manufacturer,
        device_make: device.make,
        device_model: device.model,
        device_created: device.created
    }
)
// discard any 'unknown' entries since these represent data inconsistencies in (hopefully) non-prod systems
FILTER sub.customer_id != 'unknown' && sub.building_id != 'unknown'
RETURN sub
'''

aql = aql1+aql2+aql3


print("fetching details please wait" )
db = conn["tofstenant"]
queryResult = db.AQLQuery(aql, rawResults=True)



df = pd.DataFrame(queryResult)



df1 = df[df['device_model'].isin(['4004', '4005', '4006', '4008', '4100', '4100U', '4010', '4100ES', '4010ES', '4007ES', 'P', 'PH', 'H', 'PHC', 'Duct', 'AO','AV', 'VO', 'SO','SV'])]



df1['device_created2'] = pd.to_datetime(df1['device_created'], format="%Y-%m-%dT%H:%M:%S")
df1['year'] = pd.DatetimeIndex(df1['device_created']).year


df1 = df1.drop(['device_created', 'device_created2'], axis=1)



def save_excel_sheet(df, path, sheet_name, index=False):
    # Create file if it does not exist
    if not os.path.exists(path):
        df.to_excel(path, sheet_name=sheet_name, index=index)

    # Otherwise, add a sheet. Overwrite if there exists one with the same name.
    else:
        with pd.ExcelWriter(path, engine='openpyxl', if_sheet_exists='replace', mode='a') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)


print("saving results" )            
path = r"final_208.xlsx"
df = df1
#df1['year'] = df1['year'].apply(lambda a: pd.to_datetime(a).date()) 
sheet_name = "inventory_raw_data"

save_excel_sheet(df1, path, sheet_name, index = False)



output = pd.pivot_table(data=df1, index=['customer_name', 'building_name',
                                       'device_type','device_manufacturer', 'year'], 
                        values=['device_id'],
                        columns=['device_model'],
                        aggfunc='count',
                        fill_value=''
                    )





dataframe = output
sheet_name = "inventory_pivot"

save_excel_sheet(dataframe, path, sheet_name,index = True)


print("results are saved at - " +path )

