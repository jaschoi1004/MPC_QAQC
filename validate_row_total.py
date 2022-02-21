import pandas as pd
from Base_DFs import raw_df
# The base dataframe renamed 0-N
from comprehend_rows import base_renamed_df, find_first_col, make_list_of_cols
from comprehend_rows import md_len  # Length of the rows to delete
from comprehend_rows import find_n_cols_to_delete  # List of the columns to delete
from comprehend_rows import total_rows
from comprehend_rows import rows_to_sum


# THE FOLLOWING CODE VALIDATES THE TOTAL VS CREATED TOTAL ROW VALUES


def create_data_only_df_part_1(df, number_of_cols, col_header_list):
    # This function takes in the dataframe with headers 0-N,
    # the list of columns to be deleted and the list holding all of the headers
    # This function creates a dataframe without the metadata columns
    N = number_of_cols
    del col_header_list[0:N]
    clean_col_df = df[col_header_list]
    return clean_col_df


def create_data_only_df_part_2(df, length_value):
    # This function takes in the cleaned dataframe (from which the columns of metadata were deleted)
    # and the number of metadata rows
    # and returns a dataframe without any rows of metadata
    N = length_value
    data_only_df = df.iloc[N+1:, :]
    return data_only_df


def create_data_only_df_part_3(df):
    # This function takes in the data-only dataframe
    # This makes sure the dataframe is all integers
    df = df.apply(pd.to_numeric)
    return df


def create_df_total_rows(data_only_df, rows_to_sum):
    # This function takes in the "data-only" df and the rows to sum and creates a separate df
    # with only the total rows
    total_rows_df = data_only_df.drop(labels=rows_to_sum)
    total_rows_df = total_rows_df.transpose()
    return total_rows_df


# def create_total_rows_dict(total_rows_df):
#     # This function takes in the dataframe with only the total rows
#     # This function creates a dictionary with only the total_rows
#     total_rows_list = total_rows_df.to_dict('records')
#     total_rows_dict = total_rows_list[0]
#     return total_rows_dict


def create_df_rows_to_be_aggregated(data_only_df, total_rows):
    # This function takes in the "data-only" df and the rows to sum and creates a separate df
    # with only the rows to be aggregated summed
    rows_to_be_aggregated_df = data_only_df.drop(labels=total_rows)
    created_sum_df = rows_to_be_aggregated_df.sum(axis=0)
    return created_sum_df


def create_rows_to_agg_dict(create_sum_df):
    # This function creates a dictionary of the sum of the rows to be aggregated
    created_sum_dict = create_sum_df.to_dict()
    return created_sum_dict


def create_created_sum_df(created_sum_dict):
    # This function creates a dataframe with the summed row with the column header '0'
    created_sum_df = pd.DataFrame.from_dict(created_sum_dict, orient='index')
    return created_sum_df


# def validate_totals_rows(total_rows_dict, created_sum_dict, first_col):
#     dict1 = total_rows_dict
#     dict2 = created_sum_dict
#     s = first_col
#     incorrect_list = []
#     for i in dict1:
#         if dict2[s] != i:
#             incorrect_list.append(dict2[s])
#         s += 1
#     return incorrect_list


def validate_rows(total_rows_df, total_rows_index, created_sum_df):
    # This function takes in the dataframe of the total row and
    # the dataframe of the summed rows to be aggregated
    # This function returns a list of the columns where the value between the aggregated column
    total_rows_df['aggregated'] = created_sum_df
    total_rows_df = total_rows_df.rename(columns={total_rows_index: 'Total'})
    index = total_rows_df.index
    condition = total_rows_df['aggregated'] != total_rows_df['Total']
    incorrect_indices = index[condition]
    incorrect_indices_list = incorrect_indices.tolist()
    return 'Unequal Cols:', incorrect_indices_list


# ---------------------------\
# Creating "data only" df
df_to_clean = base_renamed_df

column_headers = make_list_of_cols(df_to_clean)
columns_to_delete = find_n_cols_to_delete(raw_df)

data_only_df_pt1 = create_data_only_df_part_1(
    df_to_clean, columns_to_delete, column_headers)

data_only_df = create_data_only_df_part_2(data_only_df_pt1, md_len)

data_only_df = create_data_only_df_part_3(data_only_df)

# Calling functions to create variables to be used when validating row sums
# Dataframe with only the total row - transposed to make the row into a column
total_rows_df = create_df_total_rows(data_only_df, rows_to_sum)

# Dataframe with only the rows that should be aggregated
rows_to_agg_df = create_df_rows_to_be_aggregated(data_only_df, total_rows)
# Dictionary with the sum of the rows that should be aggregated
created_total_dict = create_rows_to_agg_dict(rows_to_agg_df)
# Dataframe with only one row (transposed to a column) that is
# the sum of the rows to be aggregated
created_total_df = create_created_sum_df(created_total_dict)

# Printing the two DFs to see if they were correctly transposed
# print(created_total_df)
# print(total_rows_df)

# Calling Find_first_col to find the index of the total column dataframe
# This will be used in the last function
total_row_index = find_first_col(total_rows_df)

# Calling the function to validate the row values
# Prints out a list of the columns where the totals don't match the aggregated values
unequal_col_list = validate_rows(
    total_rows_df, total_row_index, created_total_df)
print(unequal_col_list)
