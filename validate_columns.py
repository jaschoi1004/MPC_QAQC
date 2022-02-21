import pandas as pd
import numpy as np
from Base_DFs import data_only_df
from comprehend_columns import agg_column_data
from comprehend_columns import total_column_data
from comprehend_columns import total_col
from MD_Functions import agg_dict

# -------------CHECKING--AGGREGATION--METHOD----------------------------------

def check_agg_method(aggregation_dictionary, total_column_index):
    # This function checks if the aggregation method is 'count'
    # If the aggregation method = count, this function will return 'True'
    # That means that the following functions can be used as they work for csv's that simply sum the columns
    num_tot_col = int(total_column_index[0])
    agg_method = []
    if aggregation_dictionary.get(num_tot_col) == 'count':
        agg_method.append('True')
    else:
        agg_method.append(aggregation_dictionary[num_tot_col])
    return agg_method

# -------------CHECKING--COLUMN--ROW--COUNT---------------------------------
## this function checks if the length of the aggregating columns and total columns match up 
def check_dfs(agg_col_data, total_col_data):
    error_message = []
    if len(agg_col_data.index) != len(total_col_data.index):
        error_message.append('columns are not the same length')
    else:
        error_message.append('No Error')
    return error_message


# -------------VALIDATING--COLUMN--TOTALS----------------------------------
def validate_total(agg_col_data, total_col_data, data_df):
# this function adds the aggregating columns together and 
# checks it with the total column
    agg_col_data['summed'] = agg_col_data.iloc[:, :].sum(axis=1)
    data_df['summed'] = agg_col_data['summed']
    total_col_data['total'] = total_col_data.iloc[:, :]
    
    # true if it's equal, false if not
    comparison_column = np.where(
        agg_col_data["summed"] == total_col_data['total'], True, False)

    data_df['equal'] = comparison_column
    return data_df


def list_incorrect_column(data_df):
# This function prints out the index number where there are incorrect totals
    false_indices_list = data_only_df.index[~data_df['equal']].tolist()
    return false_indices_list

# prints out the index number according to the Excel csv
def actual_list_incorrect_column(false_indices_list):
    result = [x + 1 for x in false_indices_list]
    return result

# ---------------------------------------------------------------------------------

check_for_col_rows = check_dfs(agg_column_data, total_column_data)
print("Check for row count:", check_for_col_rows)

# Checking aggregation method
agg_method_count = check_agg_method(agg_dict, total_col)
print("Aggregation Method = Count?", agg_method_count)

# Adding the boolean column saying if the aggregate columns summed equal the total in the total column (True or False)
validated_data = validate_total(agg_column_data, total_column_data, data_only_df)
print("checking total column:\n", validated_data)

# Creating a list of the indices where the data does not equal the total listed in the total column
incorrect_col_data = list_incorrect_column(validated_data)
print("list of rows having incorrect totals:", incorrect_col_data)

# Creating a list of the rows where the data does not equal the total listed with correct rows as listed in the CSV
csv_index = actual_list_incorrect_column(incorrect_col_data)
print("list of rows having incorrect totals (CSV):", csv_index)
