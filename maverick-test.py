import pandas as pd
import datetime as dt
import numpy as np

def anomaly_func(file):
    '''
    This is a function that receives an Excel file as input an determines Stores that 
    default by having missing products higher than 20%
    '''   

    #Read Excel file
    df = pd.read_excel(file)

    # Create the 'year' column
    df['year'] = df['Period'].dt.year # create column 'year'

    # Select the highest year value
    #most_recent_year = df.loc[:,'year'].max() #= value instead
    most_recent_year = df['year'].max() # pick highest year value in df

    # Subset and create new dataframe using highest year value 
    most_recent_year_df = df[df['year']==most_recent_year] # subset dataframe with the highest year

    # Create the 'month' column in 'most_recent_year_df' dataframe
    most_recent_year_df['month'] = most_recent_year_df['Period'].dt.month # create column 'month'

    # Select the highest month value
    #most_recent_month = most_recent_year_df.loc[:,'month'].max()
    most_recent_month = most_recent_year_df['month'].max() # select highest month in that year
    
    # Subset and create a new Dataframe with recent month and year values
    most_recent_year_month_df = most_recent_year_df[(most_recent_year_df['year'] == most_recent_year) & (most_recent_year_df['month'] == most_recent_month)]

    # Create 'anomaly' column in 'most_recent_year_month_df' 
    most_recent_year_month_df['Anomaly'] = most_recent_year_month_df.apply(lambda x: 1 if x['CLOSING STOCK'] == 0 and x['SALES'] == 0 else 0, axis = 1)

    # Subset and create dataframe to select anomalies that are of value 1
    anomaly_df = most_recent_year_month_df[most_recent_year_month_df['Anomaly']==1]

    # New Dataframe to Count flagged items per store (for last month)
    anomaly_products_count = anomaly_df.groupby(['Store Name', 'Store Code','Auditors Name'])['Anomaly'].sum()
    count_df = anomaly_products_count.reset_index()
    count_df = count_df.rename(columns ={'Anomaly': 'Flagged items per store'})

    # Save names of anomaly store owners
    anomaly_store_owners = anomaly_df['Store Name'].unique().tolist()

    # New Dataframe to Count total items per store for store owners with anomalies (for last month)
    products_SKU = most_recent_year_month_df[most_recent_year_month_df['Store Name'].isin(anomaly_store_owners)]
    month_total_product_count = products_SKU.groupby(['Store Name', 'Store Code','Auditors Name'])['Anomaly'].count()
    sku_df = month_total_product_count.reset_index()
    sku_df = sku_df.rename(columns ={'Anomaly': 'Sum of items per store'})

    # Dataframe in expected format
    final_df = count_df.merge(sku_df, on=['Store Name', 'Store Code', 'Auditors Name'], suffixes=('_x', '_y'))
    final_df['Anomaly items per store / %'] = round(final_df['Flagged items per store']/ final_df['Sum of items per store'] * 100, 2)

    # Further sorting based on expected results
    global output_file
    output_file = final_df[final_df['Anomaly items per store / %']>20] # anomalies higher than 20%

    # # Create an ExcelWriter object
    # writer = pd.ExcelWriter("programming-assessment.xlsx")

    # # Save the DataFrame to a worksheet in the Excel file
    # output_file.to_excel(writer, sheet_name="anomaly-detection")

    # # Save the ExcelWriter object
    # writer.save()
    # Save dataframe to excel
    #output_file.to_excel('flagged-items-count.xlsx', index=False)

    
    return print(output_file)#print(output_file, output_file['Count SKU'].sum(), sep='\n\n\n')


def reference_check(file):
    """
    This function identifies the flagged items and traces back the history of these items over the past
    3 months
    """

    # Read Excel file as dataframe
    df = pd.read_excel(file)

    # Filter dataframe by date period of last month
    df.sort_values(by='Period', ascending=True, inplace=True) # sort by date ascending order
    end_date = df.iloc[-1,0] # select the most recent date
    start_date = end_date - pd.DateOffset(months=0) # set the starting date
    filtered_df = df.loc[(df['Period'] >= start_date) & (df['Period'] <= end_date)] # filter dataframe for last month

    # Call names of flagged stores and filter date period dataframe

    global output_file # flagged stores called from anomaly_func
    suspicious_stores = output_file['Store Code'].unique().tolist() # save store names as a list
    extended_months = filtered_df[filtered_df['Store Code'].isin(suspicious_stores)] # filter last month data by flagged store names

    # Apply flag settings to dataframe
    extended_months['Anomaly'] = extended_months.apply(lambda x: 1 if x['CLOSING STOCK'] == 0 and x['SALES'] == 0 else 0, axis = 1)

    # Subset dataframe to select anomalies that are of value 1
    extended_anomaly_df = extended_months[extended_months['Anomaly']==1]

    # Filter dataframe by date period ranging 3 months behind the last (recent) date
    start_period = end_date - pd.DateOffset(months=2) # set start period 
    df_for_3_months = df.loc[(df['Period'] >= start_period) & (df['Period'] <= end_date)] # filter dataframe for last 3 months   

    # Use "df_for_3_months" and "extended_anomaly_df" which represent data for last 3 months and data for last month respectively
    # Select the column names of interest
    column_names = ["Store Code", "Item Code"]

    # Create a new DataFrame using last month's records with the columns of interest
    df_subset = extended_anomaly_df[column_names]

    subfinal = df_for_3_months.merge(df_subset, on= column_names) # merge both dataframes on common columns, found in variable

    # Drop undesired columns
    subfinal.drop(columns=['Country','City','Previous visit date', 'Current visit date',
       'Barcode', 'SEGMENT', 'MANUFACTURER',
        'BRAND NAME', 'WEIGHT', 'Item_Unit', 'Packaging',
       'OPENING STOCK', 'PRICE'], inplace= True)
    
    # Arrange desired record in pivot table
    table2 = pd.pivot_table(subfinal, values= ['CLOSING STOCK', 'TOTAL PURCHASE', 'SALES'],
                           index= ['Store Name', 'Store Code', 'Auditors Name', 'Item Code', 'ITEM NAME', 'Channel', 'CATEGORY'],
                           columns= ['Period'])

    # # Create an ExcelWriter object
    # writer = pd.ExcelWriter("programming-assessment.xlsx")

    # # Save the first DataFrame to a worksheet in the Excel file
    # output_file.to_excel(writer, sheet_name="anomaly-detection")

    # # Save the second DataFrame to a worksheet in the Excel file
    # table2.to_excel(writer, sheet_name="history-of-flagged-items")

    # # Save the ExcelWriter object
    # writer.save()
    
    # Save dataframe to excel
    #table2.to_excel('hisory-flagged-items-revised.xlsx', index=True)

    return print(subfinal)


def sales_anomaly_func(file):
    """
    This function compares the sales value recorded by shops in the present month against the past
    4 months and determines whether the present sales value has defects
    """
    # Read Excel file
    df = pd.read_excel(file)
    
    # Subset and create dataframe with relevant columns to sales analysis
    sales_df = df[["Period", "Store Code", "Store Name", "Auditors Name", "SALES", "PRICE"]]

    # Compute non-zero sales 
    sales_df['Non-zero-sales'] = sales_df['SALES'].abs()

    # Compute sales value column
    sales_df['Sales Value'] = sales_df['Non-zero-sales']* sales_df['PRICE']

    subset = sales_df.groupby(["Period", "Store Code", "Store Name", "Auditors Name"])['Sales Value'].sum().reset_index(name='Sales Value')

    # Arrange desired record in pivot table
    table = pd.pivot_table(subset, values= ['Sales Value'],
                           index= ["Store Code", "Store Name", "Auditors Name"],
                           columns= ['Period'])
    
    new_sales = table.reset_index()#.stack()
    #new_sales.index.name = None

    # Compute mean of Sales value per store for the past 4 months
    past_4_months_df = new_sales.iloc[:, 4:8]  # NB. values in the column will change based on the months being selected. Also the heading captions
    row_average = round(past_4_months_df.mean(axis=1), 2)

    # Create new dataframe with average values over last 4 months and store details
    row_average_df = pd.DataFrame({'Average sales value for the past 4 months': row_average}) # create dataframe of series object 'row_average'
    part_newdf = new_sales.iloc[:, :3]  # subset indexed pivot table to pick out selected columns
    part_newdf.columns = part_newdf.columns.droplevel(level=1) # remove the period index
    part_newdf = part_newdf.join(row_average_df) # combine row_average df and dataframe containing details 

    # Add sales value for last month to the 'part_newdf'
    last_sales = round(new_sales.iloc[:, 8], 2)
    last_sales_df = pd.DataFrame({'Sales value for the recent month': last_sales}) # create dataframe of series object 'last_sales'
    part_newdf = part_newdf.join(last_sales_df)

    # Filter for stores based on thresholds
    threshold_value = 0.3
    part_newdf['Lower Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1-threshold_value), 2)
    part_newdf['Upper Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1+threshold_value), 2)

    # Filter and Flag
    # Create boolean masks for each condition
    within_limit = part_newdf['Sales value for the recent month'].between(part_newdf['Lower Threshold for past 4 months'], part_newdf['Upper Threshold for past 4 months'])
    upper_breach = part_newdf['Sales value for the recent month'] > part_newdf['Upper Threshold for past 4 months']
    lower_breach = part_newdf['Sales value for the recent month'] < part_newdf['Lower Threshold for past 4 months']

    # Define the corresponding flags for each condition
    conditions = [within_limit, upper_breach, lower_breach]
    choices = ['Within limit', 'Upper threshold breached', 'Lower threshold breached']

    # Use numpy.select to assign the corresponding flag to each row
    part_newdf['Sales Value Status'] = np.select(conditions, choices, default='Unknown')

    # # Open the Excel file using ExcelWriter
    # with pd.ExcelWriter('programming-assessment.xlsx', engine='openpyxl', mode='a') as writer:
    #     # Add the DataFrame as a new worksheet
    #     part_newdf.to_excel(writer, sheet_name='sales-store-checker')

    return print(part_newdf)

# def sales_anomaly_func(file):
#     """
#     This function compares the sales value recorded by shops in the present month against the past
#     4 months and determines whether the present sales value has defects
#     """
#     # Read Excel file
#     df = pd.read_excel(file)
    
#     # Subset and create dataframe with relevant columns to sales analysis
#     sales_df = df[["Period", "Store Code", "Store Name", "Auditors Name", "SALES", "PRICE"]]

#     # Compute non-zero sales 
#     sales_df['Non-zero-sales'] = sales_df['SALES'].abs()

#     # Compute sales value column
#     sales_df['Sales Value'] = sales_df['Non-zero-sales']* sales_df['PRICE']

#     subset = sales_df.groupby(["Period", "Store Code", "Store Name", "Auditors Name"])['Sales Value'].sum().reset_index(name='Sales Value')

#     # Arrange desired record in pivot table
#     table = pd.pivot_table(subset, values= ['Sales Value'],
#                            index= ["Store Code", "Store Name", "Auditors Name"],
#                            columns= ['Period'])
    
#     new_sales = table.reset_index()#.stack()

#     # Compute mean of Sales value per store for the past 4 months
#     past_4_months_df = new_sales.iloc[:, 4:8]  # NB. values in the column will change based on the months being selected. Also the heading captions. You may have to select more columns and rename to have month period sales values
#     row_average = round(past_4_months_df.mean(axis=1), 2)

#     # Create new dataframe with average values over last 4 months and store details
#     row_average_df = pd.DataFrame({'Average sales value for the past 4 months': row_average}) # create dataframe of series object 'row_average'
#     part_newdf = new_sales.iloc[:, :3]  # subset indexed pivot table to pick out selected columns
#     part_newdf.columns = part_newdf.columns.droplevel(level=1) # remove the period index     

#     #########################################################################################################
#     # Not scalable
#     # Add sales value for November to part_newdf
#     november = round(new_sales.iloc[:, 4], 2)
#     november_df = pd.DataFrame({'Sales value for November': november})
#     part_newdf = part_newdf.join(november_df)
#     # Add sales value for December to part_newdf
#     december = round(new_sales.iloc[:, 5], 2)
#     december_df = pd.DataFrame({'Sales value for December': december})
#     part_newdf = part_newdf.join(december_df)
#     # Add sales value for January to part_newdf
#     january = round(new_sales.iloc[:, 6], 2)
#     january_df = pd.DataFrame({'Sales value for January': january})
#     part_newdf = part_newdf.join(january_df)
#     # Add sales value for February to part_newdf
#     february = round(new_sales.iloc[:, 7], 2)
#     february_df = pd.DataFrame({'Sales value for February': february})
#     part_newdf = part_newdf.join(february_df)
#     # Add average sales value from November to December to part_newdf
#     part_newdf = part_newdf.join(row_average_df) # combine row_average df and dataframe containing details

#     # Add sales value for March to the 'part_newdf'
#     last_sales = round(new_sales.iloc[:, 8], 2)
#     last_sales_df = pd.DataFrame({'Sales value for March': last_sales}) # create dataframe of series object 'last_sales'
#     part_newdf = part_newdf.join(last_sales_df)
#     ######################################################################################################################

#     # Filter for stores based on thresholds
#     threshold_value = 0.3
#     part_newdf['Lower Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1-threshold_value), 2)
#     part_newdf['Upper Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1+threshold_value), 2)

#     # Filter and Flag
#     # Create boolean masks for each condition
#     within_limit = part_newdf['Sales value for March'].between(part_newdf['Lower Threshold for past 4 months'], part_newdf['Upper Threshold for past 4 months'])
#     upper_breach = part_newdf['Sales value for March'] > part_newdf['Upper Threshold for past 4 months']
#     lower_breach = part_newdf['Sales value for March'] < part_newdf['Lower Threshold for past 4 months']

#     # Define the corresponding flags for each condition
#     conditions = [within_limit, upper_breach, lower_breach]
#     choices = ['Within limit', 'Upper threshold breached', 'Lower threshold breached']

#     # Use numpy.select to assign the corresponding flag to each row
#     part_newdf['Sales Value Status'] = np.select(conditions, choices, default='Unknown')

#     # Store values that breach limits
#     full_df = part_newdf[~(part_newdf['Sales Value Status']=='Within limit')]

#     # # Open the Excel file using ExcelWriter
#     # with pd.ExcelWriter('programming-assessment.xlsx', engine='openpyxl', mode='a') as writer:
#     #     # Add the DataFrame as a new worksheet
#     #     full_df.to_excel(writer, sheet_name='sales-store-checker-new')


#     return part_newdf[~(part_newdf['Sales Value Status']=='Within limit')]
    


def item_sales_anomaly_func(file):
    """
    This function compares the sales value recorded by shops in the present month against the past
    4 months and determines whether the present sales value has defects
    """
    # Read Excel file
    df = pd.read_excel(file)
    
    # Subset and create dataframe with relevant columns to sales analysis
    sales_df = df[["Period", "Item Code", "ITEM NAME", "CATEGORY", "Auditors Name", "SALES", "PRICE"]]

    # Compute non-zero sales 
    sales_df['Non-zero-sales'] = sales_df['SALES'].abs()

    # Compute sales value column
    sales_df['Sales Value'] = sales_df['Non-zero-sales']* sales_df['PRICE']

    subset = sales_df.groupby(["Period", "Item Code", "ITEM NAME", "CATEGORY", "Auditors Name"])['Sales Value'].sum().reset_index(name='Sales Value')

    # Arrange desired record in pivot table
    table = pd.pivot_table(subset, values= ['Sales Value'],
                           index= ["Item Code", "ITEM NAME", "CATEGORY", "Auditors Name"],
                           columns= ['Period'])
    
    new_sales = table.reset_index()#.stack()

    # Compute mean of Sales value per store for the past 4 months
    past_4_months_df = new_sales.iloc[:, 5:9]  # NB. values in the column will change based on the months being selected. Also the heading captions. You may have to select more columns and rename to have month period sales values
    row_average = round(past_4_months_df.mean(axis=1), 2)

    # Create new dataframe with average values over last 4 months and store details
    row_average_df = pd.DataFrame({'Average sales value for the past 4 months': row_average}) # create dataframe of series object 'row_average'
    part_newdf = new_sales.iloc[:, :4]  # subset indexed pivot table to pick out selected columns
    part_newdf.columns = part_newdf.columns.droplevel(level=1) # remove the period index     

    # #########################################################################################################
    # Not scalable
    # Add sales value for November to part_newdf
    november = round(new_sales.iloc[:, 5], 2)
    november_df = pd.DataFrame({'Sales value for November': november})
    part_newdf = part_newdf.join(november_df)
    # Add sales value for December to part_newdf
    december = round(new_sales.iloc[:, 6], 2)
    december_df = pd.DataFrame({'Sales value for December': december})
    part_newdf = part_newdf.join(december_df)
    # Add sales value for January to part_newdf
    january = round(new_sales.iloc[:, 7], 2)
    january_df = pd.DataFrame({'Sales value for January': january})
    part_newdf = part_newdf.join(january_df)
    # Add sales value for February to part_newdf
    february = round(new_sales.iloc[:, 8], 2)
    february_df = pd.DataFrame({'Sales value for February': february})
    part_newdf = part_newdf.join(february_df)
    # Add average sales value from November to December to part_newdf
    part_newdf = part_newdf.join(row_average_df) # combine row_average df and dataframe containing details

    # Add sales value for March to the 'part_newdf'
    last_sales = round(new_sales.iloc[:, 9], 2)
    last_sales_df = pd.DataFrame({'Sales value for March': last_sales}) # create dataframe of series object 'last_sales'
    part_newdf = part_newdf.join(last_sales_df)
    # ######################################################################################################################

    # Filter for stores based on thresholds
    threshold_value = 0.3
    part_newdf['Lower Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1-threshold_value), 2)
    part_newdf['Upper Threshold for past 4 months'] = round(part_newdf['Average sales value for the past 4 months']* (1+threshold_value), 2)

    # Filter and Flag
    # Create boolean masks for each condition
    within_limit = part_newdf['Sales value for March'].between(part_newdf['Lower Threshold for past 4 months'], part_newdf['Upper Threshold for past 4 months'])
    upper_breach = part_newdf['Sales value for March'] > part_newdf['Upper Threshold for past 4 months']
    lower_breach = part_newdf['Sales value for March'] < part_newdf['Lower Threshold for past 4 months']

    # Define the corresponding flags for each condition
    conditions = [within_limit, upper_breach, lower_breach]
    choices = ['Within limit', 'Upper threshold breached', 'Lower threshold breached']

    # Use numpy.select to assign the corresponding flag to each row
    part_newdf['Sales Value Status'] = np.select(conditions, choices, default='Unknown')

    # Store values that breach limits
    full_df = part_newdf[~(part_newdf['Sales Value Status']=='Within limit')]

    # # Open the Excel file using ExcelWriter
    # with pd.ExcelWriter('programming-assessment.xlsx', engine='openpyxl', mode='a') as writer:
    #     # Add the DataFrame as a new worksheet
    #     full_df.to_excel(writer, sheet_name='sales-item-checker')

    return full_df


anomaly_func(r"C:\Users\Akosua\Documents\Andrew\DS Maverick Sample Dataset.xlsx")
reference_check(r"C:\Users\Akosua\Documents\Andrew\DS Maverick Sample Dataset.xlsx")
sales_anomaly_func(r"C:\Users\Akosua\Documents\Andrew\DS Maverick Sample Dataset.xlsx")
item_sales_anomaly_func(r"C:\Users\Akosua\Documents\Andrew\DS Maverick Sample Dataset.xlsx")


